import type WS from 'ws';
import { WebSocket } from 'ws';
import express, { type Express, type Request } from 'express';
import { verifyRequestSignature } from '../auth/authenticator';
import { Session } from '../common/session';
import { getPort } from '../common/environment-variables';
import { SecretService } from '../services/secret-service';
import { Ingestion } from './ingestion_tasks';

export class Server {
    private app: Express | undefined;
    private httpServer: any;
    private wsServer: any;
    private connections: Map<string, Ingestion> = new Map(); // Mapa de conexões
    private secretService = new SecretService();
    
    start() {
        console.log(`Starting server on port: ${getPort()}`);

        this.app = express();
        this.httpServer = this.app.listen(getPort());
        this.wsServer = new WebSocket.Server({
            noServer: true
        });

        this.httpServer.on('upgrade', (request: Request, socket: any, head: any) => {
            console.log(`Received a connection request from ${request.url}.`);

            verifyRequestSignature(request, this.secretService) // Verifique a assinatura da requisição
                .then(verifyResult => {
                    if (verifyResult.code !== 'VERIFIED') {
                        console.log('Authentication failed, closing the connection.');
                        socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
                        socket.destroy();
                        return;
                    }

                    this.wsServer.handleUpgrade(request, socket, head, (ws: WebSocket) => {
                        console.log('Authentication was successful.');
                        this.wsServer.emit('connection', ws, request);
                    });
                });
        });

        this.wsServer.on('connection', (ws: WebSocket, request: Request) => {
            const mac_operation_id = 5; // Defina o ID da operação
            const ingestor = new Ingestion(ws, this.connections, mac_operation_id);

            ws.on('close', () => {
                console.log('WebSocket connection closed.');
                this.deleteConnection(ingestor.id_call); // Remova a conexão
            });

            ws.on('error', (error: Error) => {
                console.log(`WebSocket Error: ${error}`);
                ws.close();
                this.deleteConnection(ingestor.id_call); // Remova a conexão em caso de erro
            });

            ws.on('message', (data: WS.RawData, isBinary: boolean) => {
                if (ws.readyState !== WebSocket.OPEN) {
                    return;
                }

                if (isBinary) {
                    ingestor.processBinaryMessage(data as Uint8Array); // Processa mensagem binária
                } else {
                    ingestor.processTextMessage(data.toString()); // Processa mensagem de texto
                }
            });

            this.createConnection(ingestor); // Cria a conexão
        });

        // Endpoint para informações do WebSocket
        this.app?.get("/audiohook/automacall-levesaude/info", async (req, res) => {
            res.json({
                "description": "Esse endpoint é utilizado para a comunicação entre telefonia (Genesys) e o Audio Ingestor.",
                "url": "/audiohook/automacall-levesaude",
                "notes": "Este endpoint aceita apenas conexões WebSocket."
            });
        });

        // Endpoint WebSocket para quantidade de ligações
        this.app?.ws("/qtd-ligacoes-momento", async (ws, req) => {
            console.log("(ING) Nova conexão WebSocket para monitoramento de quantidade de ligações");
            await this.monitor_qtd_ligacoes(ws);
        });
    }

    private createConnection(ingestor: Ingestion) {
        if (this.connections.has(ingestor.id_call)) {
            return;
        }

        console.log('Creating a new connection.');
        this.connections.set(ingestor.id_call, ingestor); // Adicione a conexão ao mapa
    }

    private deleteConnection(id_call: string) {
        if (!this.connections.has(id_call)) {
            return;
        }

        const ingestor = this.connections.get(id_call);

        try {
            ingestor?.close(); // Fecha a conexão
        } catch (error) {
            console.error("Erro ao fechar a conexão:", error);
        }

        console.log('Deleting connection.');
        this.connections.delete(id_call); // Remove a conexão do mapa
    }

    private async monitor_qtd_ligacoes(ws: WebSocket) {
        try {
            while (true) {
                const qtd_calls = this.connections.size; // Obtem a quantidade de conexões
                ws.send(`${qtd_calls}`); // Envia a quantidade de conexões
                await new Promise(resolve => setTimeout(resolve, 1000)); // Aguarda 1 segundo
            }
        } catch (error) {
            console.error("(ING) Problema no monitor de qtd de ligações:", error);
        }
    }
}