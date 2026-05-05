import { Injectable } from '@angular/core';
import { Client, IMessage, StompSubscription, IFrame } from '@stomp/stompjs';
import { Observable, Subject, BehaviorSubject } from 'rxjs';

export interface StompConfig {
  /** URL WebSocket du broker, ex: ws://localhost:15674/ws */
  brokerURL: string;
  /** Login RabbitMQ */
  login: string;
  /** Mot de passe RabbitMQ */
  passcode: string;
  /** Routing key (ou destination) utilisée pour l'envoi des messages */
  sendRoutingKey: string;
  /** Une ou plusieurs destinations STOMP à écouter (queues, topics, exchanges) */
  listenQueues: string[];
  /** Virtual host RabbitMQ (par défaut: '/') */
  host?: string;
  /** Délai de reconnexion automatique en ms (par défaut: 5000) */
  reconnectDelay?: number;
  /** Heartbeat entrant en ms (par défaut: 10000) */
  heartbeatIncoming?: number;
  /** Heartbeat sortant en ms (par défaut: 10000) */
  heartbeatOutgoing?: number;
  /** Activer les logs de debug */
  debug?: boolean;
}

export type MessageCallback = (message: IMessage) => void;

@Injectable({
  providedIn: 'root',
})
export class StompService {
  private client: Client | null = null;
  private config: StompConfig | null = null;
  private subscriptions = new Map<string, StompSubscription>();
  private callbacks = new Map<string, MessageCallback[]>();

  private readonly connectionState$ = new BehaviorSubject<boolean>(false);
  private readonly errors$ = new Subject<IFrame | Error>();

  /** Observable de l'état de connexion (true = connecté) */
  public readonly isConnected$: Observable<boolean> = this.connectionState$.asObservable();
  /** Observable des erreurs STOMP */
  public readonly onError$: Observable<IFrame | Error> = this.errors$.asObservable();

  /**
   * Initialise et connecte le client STOMP avec la configuration fournie.
   */
  public connect(config: StompConfig): void {
    if (this.client?.active) {
      console.warn('[StompService] Client déjà connecté. Déconnexion préalable nécessaire.');
      return;
    }

    this.config = config;

    this.client = new Client({
      brokerURL: config.brokerURL,
      connectHeaders: {
        login: config.login,
        passcode: config.passcode,
        host: config.host ?? '/',
      },
      reconnectDelay: config.reconnectDelay ?? 5000,
      heartbeatIncoming: config.heartbeatIncoming ?? 10000,
      heartbeatOutgoing: config.heartbeatOutgoing ?? 10000,
      debug: config.debug ? (msg) => console.log('[STOMP]', msg) : () => {},
    });

    this.client.onConnect = (frame) => {
      console.log('[StompService] Connecté au broker', frame);
      this.connectionState$.next(true);
      this.subscribeToConfiguredQueues();
    };

    this.client.onStompError = (frame) => {
      console.error('[StompService] Erreur STOMP:', frame.headers['message'], frame.body);
      this.errors$.next(frame);
    };

    this.client.onWebSocketError = (event) => {
      console.error('[StompService] Erreur WebSocket:', event);
      this.errors$.next(new Error('WebSocket error'));
    };

    this.client.onDisconnect = () => {
      console.log('[StompService] Déconnecté');
      this.connectionState$.next(false);
    };

    this.client.activate();
  }

  /**
   * Souscrit automatiquement à toutes les queues définies dans la configuration.
   */
  private subscribeToConfiguredQueues(): void {
    if (!this.config) return;
    this.config.listenQueues.forEach((queue) => {
      if (!this.subscriptions.has(queue)) {
        this.subscribe(queue);
      }
    });
  }

  /**
   * Abonne le client à une destination STOMP et permet d'enregistrer un callback.
   * Plusieurs callbacks peuvent être enregistrés pour une même destination.
   *
   * @param destination Destination STOMP (ex: /queue/myqueue, /exchange/myexchange/key)
   * @param callback Fonction appelée à chaque réception de message (optionnelle)
   */
  public subscribe(destination: string, callback?: MessageCallback): void {
    if (!this.client?.connected) {
      console.warn(`[StompService] Impossible de s'abonner à ${destination}: client non connecté`);
      if (callback) this.addCallback(destination, callback);
      return;
    }

    if (callback) this.addCallback(destination, callback);

    if (this.subscriptions.has(destination)) {
      return;
    }

    const subscription = this.client.subscribe(destination, (message: IMessage) => {
      const callbacks = this.callbacks.get(destination) ?? [];
      callbacks.forEach((cb) => {
        try {
          cb(message);
        } catch (err) {
          console.error(`[StompService] Erreur dans le callback pour ${destination}:`, err);
        }
      });
    });

    this.subscriptions.set(destination, subscription);
    console.log(`[StompService] Abonné à ${destination}`);
  }

  /**
   * Enregistre un callback pour une destination donnée.
   */
  private addCallback(destination: string, callback: MessageCallback): void {
    const existing = this.callbacks.get(destination) ?? [];
    existing.push(callback);
    this.callbacks.set(destination, existing);
  }

  /**
   * Désabonne le client d'une destination et supprime ses callbacks.
   */
  public unsubscribe(destination: string): void {
    const subscription = this.subscriptions.get(destination);
    if (subscription) {
      subscription.unsubscribe();
      this.subscriptions.delete(destination);
      this.callbacks.delete(destination);
      console.log(`[StompService] Désabonné de ${destination}`);
    }
  }

  /**
   * Envoie un message sur la routing key configurée (ou une destination personnalisée).
   *
   * @param body Corps du message (string ou objet sérialisé en JSON)
   * @param destination Destination optionnelle, sinon utilise la sendRoutingKey de la config
   * @param headers Headers STOMP optionnels
   */
  public send(
    body: string | object,
    destination?: string,
    headers: Record<string, string> = {}
  ): void {
    if (!this.client?.connected) {
      console.error('[StompService] Impossible d\'envoyer: client non connecté');
      return;
    }

    const target = destination ?? this.config?.sendRoutingKey;
    if (!target) {
      console.error('[StompService] Aucune destination spécifiée');
      return;
    }

    const payload = typeof body === 'string' ? body : JSON.stringify(body);
    const finalHeaders =
      typeof body === 'object' && !headers['content-type']
        ? { ...headers, 'content-type': 'application/json' }
        : headers;

    this.client.publish({
      destination: target,
      body: payload,
      headers: finalHeaders,
    });
  }

  /**
   * Déconnecte proprement le client et nettoie les abonnements.
   */
  public async disconnect(): Promise<void> {
    if (!this.client) return;

    this.subscriptions.forEach((sub) => sub.unsubscribe());
    this.subscriptions.clear();
    this.callbacks.clear();

    await this.client.deactivate();
    this.client = null;
    this.connectionState$.next(false);
    console.log('[StompService] Client désactivé');
  }

  /**
   * Indique si le client est actuellement connecté.
   */
  public get connected(): boolean {
    return this.client?.connected ?? false;
  }
}
