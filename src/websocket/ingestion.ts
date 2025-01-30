import type { WebSocket } from 'ws';
import { Queue } from 'async-queue'; // Importe a biblioteca async-queue
import { Triagem } from './triagem'; // Importe a classe Triagem
import { Session } from '../common/session'; // Importe a classe Session
import { getPort } from '../common/environment-variables'; // Importe a função para obter a porta
import { SecretService } from '../services/secret-service'; // Importe o serviço de segredos
import { converter } from './utils/wav_converter'; // Importe a função converter
import { BackgroundNoise } from './utils/mulaw_n_noise_gen'; // Importe a classe BackgroundNoise
import * as AWS from 'aws-sdk'; // Importe o SDK da AWS
import { MessageHandlerRegistry } from './message-handlers/message-handler-registry'; //Ainda não refatorei as mensagens para os handlers que vem no projeto base deles, eles seguem o pattern strategy

const ORCHESTRATOR_WEBSOCKET = "ws://localhost:9999/orquestration/orquestration";

// Configuração do SDK da AWS
const s3 = new AWS.S3({
    accessKeyId: process.env.ACCESS_KEY,
    secretAccessKey: process.env.SECRET_KEY,
    region: process.env.AWS_REGION,
});

const AUDIO_RECORDER_BUCKET = process.env.AUDIO_RECORDER_BUCKET;
console.log(`(ING) BUCKET para salvar audios: ${AUDIO_RECORDER_BUCKET}`);

export class Ingestion {

    webskt_genesys: WebSocket;
    webskt_orquestration: WebSocket | null = null;

    pkts_audios_buffer: Queue<any> = new Queue();
    pkts_meta_buffer: Queue<any> = new Queue();
    pkts_meta_tosend_gen_buffer: Queue<any> = new Queue();
    pkts_audios_tosend_gen_buffer: Queue<any> = new Queue();

    connections: Map<string, Ingestion>;

    mac_operation_id: number;
    id_call: string | null = null;
    id_session: string | null = null;
    client_number: string | null = null;
    metadata_pkt: any | null = null;

    end_call = false;

    call_audio_buffer_client: Buffer = Buffer.alloc(0);
    call_audio_buffer_ai: Buffer = Buffer.alloc(0);

    triagem: Triagem | null = null;
    background: BackgroundNoise = new BackgroundNoise();
    playback_completed = false;
    audio_sent = false;
    ignore_plbk_completed = true;

    constructor(webskt_connection: WebSocket, connections: Map<string, Ingestion>, mac_operation_id: number) {
        this.webskt_genesys = webskt_connection;
        this.connections = connections;
        this.mac_operation_id = mac_operation_id;

        console.log("(ING) Objeto Ingestion instanciado para nova conexão Genesys");
    }

    async receive_metadata_pkt_from_genesys(): Promise<void> {
        const first_pkt = await this.webskt_genesys.receive();
        const first_pkt_json = JSON.parse(first_pkt.text);

        console.log(`(ING) Open PKT: ${first_pkt_json}`);
        this.id_call = first_pkt_json.parameters.conversationId;
        this.id_session = first_pkt_json.id;
        this.client_number = first_pkt_json.parameters.participant.ani;
        this.metadata_pkt = first_pkt_json;

        this.triagem = new Triagem(this.id_session, this.metadata_pkt);

        const rcvd_data = {
            id: this.id_call,
            flag: "OPEN",
            details: this.metadata_pkt,
        };

        await this.pkts_meta_buffer.put(rcvd_data);
    }

    private messageHandlerRegistry: MessageHandlerRegistry = new MessageHandlerRegistry();

    async task0_connect_orquestration(): Promise<void> {
        try {
            this.webskt_orquestration = await connect(ORCHESTRATOR_WEBSOCKET);
            this.metadata_pkt.mac_operation_id = this.mac_operation_id;
            const pkt = JSON.stringify(this.metadata_pkt);
            this.webskt_orquestration.send(pkt);
        } catch (e) {
            console.error(`(ING) Erro ao conectar com a orquestração: ${e}`);
            throw e;
        }
    }

    async task1_receive_pkts_genesys(): Promise<void> {
        try {
            while (!this.end_call) {
                const data = await this.webskt_genesys.receive();

                if (data.binary) {
                    this.call_audio_buffer_client = Buffer.concat([this.call_audio_buffer_client, data.bytes]);
                    const send_data = { id: this.id_call, flag: "MEDIA", details: data.bytes };
                    await this.pkts_audios_buffer.put(send_data);
                } else if (data.text) {
                    const send_data = {
                        id: this.id_call,
                        flag: "METADATA",
                        details: data.text,
                    };
                    await this.pkts_meta_buffer.put(send_data);
                } else if (data.type === "websocket.disconnect") {
                    this.end_call = true;

                    // Salvar áudios no S3
                    const paramsClient = {
                        Bucket: AUDIO_RECORDER_BUCKET,
                        Key: `${this.id_call}_cliente.wav`,
                        Body: converter.convert(this.call_audio_buffer_client),
                    };

                    const paramsAI = {
                        Bucket: AUDIO_RECORDER_BUCKET,
                        Key: `${this.id_call}_ai.wav`,
                        Body: converter.convert(this.call_audio_buffer_ai),
                    };

                    await s3.upload(paramsClient).promise();
                    await s3.upload(paramsAI).promise();

                    this.connections.delete(this.id_call as string);
                } else {
                    console.warn(`(ING) ${this.id_call} - Unknown packet rcvd: ${data}`);
                    const send_data = { id: this.id_call, flag: "UNKNOWN", details: data };
                    await this.pkts_meta_buffer.put(send_data);
                }
            }
        } catch (e) {
            console.error(`(ING) ${this.id_call} - Erro em _receive_pkt: ${e}`);
            throw e;
        }
    }

    async task2_analyze_meta_pkts(): Promise<void> {
        try {
            while (!this.end_call) {
                const pkt = await this.pkts_meta_buffer.get();

                if (typeof pkt.details === 'string') {
                    pkt.details = JSON.parse(pkt.details);
                }

                const response = this.triagem?.is_open_or_metadata(pkt);

                if (pkt.details.type === "playback_completed" && !this.ignore_plbk_completed) {
                    pkt.flag = "END_AUDIO";

                    if (!this.ignore_plbk_completed) {
                        this.playback_completed = true;
                        this.ignore_plbk_completed = true;
                    }

                    console.log(`(ING) ${this.id_call} - Playback completed recebido`);
                    console.log(`(ING) ${this.id_call} - Enviando END_AUDIO para orquestração: ${pkt}`);
                    const pkt_str = JSON.stringify(pkt);
                    this.webskt_orquestration?.send(pkt_str);
                } else if (pkt.details.type === "close") {
                    pkt.flag = "END_CALL";
                    console.log(`(ING) ID: ${this.id_call} - FIM DA CHAMADA`);
                    const pkt_str = JSON.stringify(pkt);
                    this.webskt_orquestration?.send(pkt_str);
                }

                if (response) {
                    await this.pkts_gen_buffer.put(response);
                }
            }
        } catch (e) {
            console.error(`(ING) ${this.id_call} - Seguinte problema em triagem_pkts_rcvd: ${e}`);
            throw e;
        }
    }

    async task3_send_audio_orquestration_pkts(): Promise<void> {
        try {
            while (!this.end_call) {
                const pkt = await this.pkts_audios_buffer.get();
                
                if (pkt.flag !== "MEDIA") {
                    console.warn(`Pacote não é de áudio: ${pkt}`);
                    continue; // Ignora pacotes que não são de áudio
                }

                this.webskt_orquestration?.send(pkt.details);
            }
        } catch (e) {
            console.error(`(ING) ${this.id_call} - Erro em task3_send_audio_orquestration_pkts: ${e}`);
            throw e;
        }
    }

    async task4_meta_pkt_sender(): Promise<void> {
        try {
            while (!this.end_call) {
                const pkt = await this.pkts_meta_tosend_gen_buffer.get();
                const pkt_str = JSON.stringify(pkt.details);
                this.webskt_genesys.send(pkt_str);
            }
        } catch (e) {
            console.error(`(ING) ${this.id_call} - Erro no task4_meta_pkt_sender: ${e}`);
            throw e;
        }
    }

    async task5_audio_pkt_sender(): Promise<void> {
        try {
            while (!this.end_call) {
                try {
                    const pkt = await this.pkts_audios_tosend_gen_buffer.get();

                    if (pkt.details === "EOF") {
                        console.log(`(ING) ${this.id_call} - EOF recebido - desbloqueando playback completed`);
                        this.ignore_plbk_completed = false;
                        continue;
                    }

                    if (pkt.flag === "SAY") {
                        const audio = pkt.details;
                        this.webskt_genesys.send(audio);
                        this.call_audio_buffer_ai = Buffer.concat([this.call_audio_buffer_ai, audio]);
                        await new Promise(resolve => setTimeout(resolve, 500)); // Aguarda 0.5 segundos
                    }
                } catch (asyncio.TimeoutError) 
                    if (this.ignore_plbk_completed && !this.playback_completed) {
                        const noise_chunk = this.background.noise_generation();
                        this.webskt_genesys.send(noise_chunk);
                        this.call_audio_buffer_ai = Buffer.concat([this.call_audio_buffer_ai, noise_chunk]);
                    } else if (this.playback_completed) {
                        console.log("(ING) Resetando variáveis de controle para envio de ruído - playback completed");
                        this.playback_completed = false;
                    } else {
                    }
            }
        } catch (e) {
            console.error(`(ING) Erro no task5_audio_pkt_sender: ${e}`);
            throw e;
        }
    }

    async task6_receive_pkts_orquestration(): Promise<void> {
        if (!this.webskt_orquestration) {
            throw new Error("Objeto de orquestração não instanciado");
        }

        try {
            while (!this.end_call) {
                const data = await this.webskt_orquestration.receive();
                const data_json = JSON.parse(data);

                if (data_json.flag === "SAY") {
                    const audio = Buffer.from(data_json.details, 'base64');
                    const pkt = { flag: "SAY", details: audio };
                    await this.pkts_audios_tosend_gen_buffer.put(pkt);
                } else if (data_json.flag === "EOF") {
                    await this.pkts_audios_tosend_gen_buffer.put(data_json);
                } else if (data_json.flag === "END") {
                    const response = this.triagem?.end_call();
                    await this.pkts_meta_tosend_gen_buffer.put(response);
                } else {
                    console.warn(`(ING) Pacote de metadados recebido não previsto: ${data_json}`);
                }
            }
        } catch (e) {
            console.error(`(ING) Erro em task6_receive_pkts_orquestration: ${e} - datatype: ${typeof data}`);
            throw e;
        }
    }

    close() {
        this.end_call = true;
        this.webskt_genesys.close();
        this.webskt_orquestration?.close();
    }
}