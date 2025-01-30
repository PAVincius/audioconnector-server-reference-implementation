import * as fs from 'node:fs';
import * as path from 'node:path';
import { Logger } from 'tslog';

const log = new Logger();

const MESSAGES = path.join(__dirname, "operational", "Genesys", "ingestion", "messages"); // Ajuste o caminho conforme necessário

export class Triagem {
    open_pkt: any;
    server_pkt_send_counter = 1;
    client_seq_lastseen = 0;
    id_session: string;
    id_call: string | null = null;
    templates: { [key: string]: any } = {};
    templates_keys: string[] = [];

    constructor(id_session: string, open_pkt: any, schema_folder: string = MESSAGES) {
        this.id_session = id_session;
        this.open_pkt = open_pkt;
        this.__load_templates_schemas(schema_folder);
    }

    is_open_or_metadata(pkt: any): any {
        try {
            if (pkt.flag === "OPEN") {
                this.id_call = pkt.details.parameters.conversationId;
                const response = this.opened_pkt();
                log.info(`(ING) ${this.id_call} - Recebido OPEN:`, response);
                log.debug(`(ING) ${this.id_call} - Enviado: ${response.details.type}\n\n`);
                return response;
            }if (pkt.flag === "METADATA") {
                const response = this.verify_metadata_pkts(pkt.details);
                if (response && Array.isArray(response) && response[0] === "END") {
                    return response[1];
                }if (response) {
                    return response;
                }
            }
        } catch (e) {
            log.error(`(ING) ${this.id_call} - Problema em triagem_pkts_tcvd: ${e}`);
        }
    }

    verify_metadata_pkts(pkt_payload: any): any {
        let response = null;

        switch (pkt_payload.type) {
            case "ping":
                response = this.__pong_pkt();
                break;
            case "close":
                response = this.__closed_pkt();
                break;
            case "error":
                log.error(`(ING) ${this.id_call} - Erro recebido: ${pkt_payload.parameters.message}`);
                break;
            case "discarded":
                log.info(`(ING) ${this.id_call} - Pacote descartado recebido: ${pkt_payload}`);
                break;
            case "playback_started":
                log.info(`(ING) ${this.id_call} - Playback started recebido!`);
                break;
            case "playback_completed":
                log.info(`(ING) ${this.id_call} - Playback completed recebido!`);
                break;
            default:
                log.warn(`(ING) ${this.id_call} - Tipo de pacote de metadados desconhecido: ${pkt_payload}`);
        }

        this.client_seq_lastseen = pkt_payload.seq;

        if (response) {
            this.server_pkt_send_counter++;
        }
        return response;
    }

    __load_templates_schemas(schema_folder: string): void {
        const schemas = fs.readdirSync(schema_folder);
        schemas.forEach(schema => {
            const path_ = path.join(schema_folder, schema);
            const file = fs.readFileSync(path_, 'utf8');
            this.templates[schema] = JSON.parse(file);
        });
        this.templates_keys = Object.keys(this.templates);
    }

    __prepare_response(response_type: string): any {
        if (!this.templates_keys.includes(response_type)) {
            throw new Error(`Tipo de pacote de resposta desconhecido: ${response_type} - disponíveis: ${this.templates_keys}`);
        }
        const base_response = this.templates[response_type];
        base_response.id = this.id_session;
        base_response.seq = this.server_pkt_send_counter;
        base_response.clientseq = this.client_seq_lastseen;

        return { id: this.id_session, flag: "OPEN", details: base_response };
    }

    opened_pkt(): any {
        const response = this.__prepare_response("opened.json");
        this.server_pkt_send_counter++;
        return response;
    }

    __pong_pkt(): any {
        return this.__prepare_response("pong.json");
    }

    __closed_pkt(): [string, any] {
        const response = this.__prepare_response("closed.json");
        return ["END", response];
    }

    end_call(): any {
        const response = this.__prepare_response("disconnect.json");
        response.details.parameters.outputVariables.idURL = this.id_call;
        this.server_pkt_send_counter++;
        return response;
    }
}