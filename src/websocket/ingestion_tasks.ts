import { type WebSocket, connect } from 'ws';
import { Queue } from 'async-queue';
import { Triagem } from './triagem';
import { Session } from '../common/session'; // Importe a classe Session
import { getPort } from '../common/environment-variables'; // Importe a função para obter a porta
import { SecretService } from '../services/secret-service'; // Importe o serviço de segredos
import { converter } from './utils/wav_converter';
import { BackgroundNoise } from './utils/mulaw_n_noise_gen';
import * as AWS from 'aws-sdk';
import { MessageHandlerRegistry } from './message-handlers/message-handler-registry';
import type { ClientMessage } from '../../protocol/message'; // Importe o tipo ClientMessage

const ORCHESTRATOR_WEBSOCKET = "ws://localhost:9999/orquestration/orquestration"; // depois mudar para websocket do server.ts

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

    end_call: boolean = false;

    call_audio_buffer_client: Buffer = Buffer.alloc(0);
    call_audio_buffer_ai: Buffer = Buffer.alloc(0);

    triagem: Triagem | null = null;
    background: BackgroundNoise = new BackgroundNoise();
    playback_completed: boolean = false;
    audio_sent: boolean = false;
    ignore_plbk_completed: boolean = true;

    private readonly messageHandlerRegistry: MessageHandlerRegistry = new MessageHandlerRegistry();

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

        const rcvd_data: ClientMessage = { // Tipagem ClientMessage
            id: this.id_call,
            flag: "OPEN",
            details: this.metadata_pkt,
        };

        await this.pkts_meta_buffer.put(rcvd_data);
    }

    async task0_connect_orquestration(): Promise<void> {
        try {
            this.webskt_orquestration = await connect(ORCHESTRATOR_WEBSOCKET);
            this.metadata_pkt.mac_operation_id = this.mac_operation_id;

            const openMessage: ClientMessage = { // Tipagem ClientMessage
                flag: "OPEN",
                details: this.metadata_pkt
            };

            const handler = this.messageHandlerRegistry.getHandler('open');

            if (handler) {
                handler.handleMessage(openMessage, /* passe a sessão ou outros dados necessários */);
            } else {
                console.warn(`(ING) ${this.id_call} - No handler found for message type: open`);
            }

        } catch (e) {
            console.error(`(ING) Erro ao conectar com a orquestração: ${e}`);
            throw e;
        }
    }

    
}