/* ================================================================
   ATLAS — configuration Highcharts Gantt (production)
   ----------------------------------------------------------------
   Correspond à la maquette v4 : une ligne par identité (séjours
   consolidés), dépliable en sous-lignes de contributions par
   logique. Divergence en surcouche pattern-fill. Axe temporel
   inversé pour le RTL hébreu.

   Modules requis (bundle Gantt — licence Highcharts Gantt) :
     import Highcharts from 'highcharts/highcharts-gantt';
     import PatternFill from 'highcharts/modules/pattern-fill';
     import Accessibility from 'highcharts/modules/accessibility';
     PatternFill(Highcharts); Accessibility(Highcharts);

   Angular : wrapper officiel highcharts-angular
     <highcharts-chart [Highcharts]="Highcharts"
        [constructorType]="'ganttChart'" [options]="options">
   ================================================================ */

/* ----------------------------------------------------------------
   1. Modèle d'entrée (ce que renvoie votre API C#)
   ----------------------------------------------------------------
   identity = {
     key: 'yas', name: 'כהן יסמינה',
     kind: 'person' | 'orphan' | 'quarantine',
     stays: [{
       country: 'es', countryName: 'ספרד',
       from: 1739577600000, to: 1743897600000,   // ms UTC, borne fin EXCLUSIVE
       confidence: 'h' | 'm' | 'f', nSources: 3,
       divergence: { country:'pt', countryName:'פורטוגל',
                     from:…, to:…, confidence:'f', nSources:1 } | null,
       contributions: [{
         logic: 'איכון רשת',
         country: 'es',                           // pays AFFIRMÉ par la source
         intervals: [{from:…, to:…, point:false}] // point:true = événement ponctuel
       }]
     }]
   }
   ---------------------------------------------------------------- */

const COUNTRY_COLORS = {
  fr: '#3B6FB6', es: '#E2A72E', ma: '#2E8C7F',
  pt: '#9C5FA0', ot: '#97A4AE'
};
const CONF_LABEL = { h: 'גבוהה', m: 'בינונית', f: 'נמוכה' };

const faded = hex => {
  const n = parseInt(hex.slice(1), 16);
  return `rgba(${n >> 16},${(n >> 8) & 255},${n & 255},0.38)`;
};

/* hachure « divergence » : trait du pays concurrent sur fond rougeâtre */
const divergencePattern = countryColor => ({
  pattern: {
    path: { d: 'M 0 8 L 8 0', strokeWidth: 2.5 },
    width: 8, height: 8,
    color: countryColor,
    backgroundColor: 'rgba(179,64,58,.15)'
  }
});

/* hachure « quarantaine » (ambre) */
const QUAR_PATTERN = {
  pattern: {
    path: { d: 'M 0 8 L 8 0', strokeWidth: 3 },
    width: 8, height: 8,
    color: '#C89B3C', backgroundColor: '#FCF3DF'
  }
};

/* ----------------------------------------------------------------
   2. Mapping identités → points Gantt
   ----------------------------------------------------------------
   Principe treegrid :
   - la LIGNE identité est un nœud dont l'id est identity.key ;
     le PREMIER séjour porte cet id, les séjours suivants portent
     le même `name` → avec yAxis.uniqueNames=true ils partagent
     la même ligne. (Pattern documenté « multiple bars per row ».)
   - chaque logique est un nœud enfant (parent = identity.key) ;
     ses intervalles partagent le même name sous le même parent
     → une sous-ligne par logique.
   - le treegrid fournit le repli/dépli natif (▸) — c'est
     l'équivalent du bouton d'expansion de la maquette.
   ---------------------------------------------------------------- */
export function buildSeries(identities) {
  const stayPoints = [];
  const divPoints = [];
  const contribPoints = [];

  identities.forEach(id => {
    id.stays.forEach((s, i) => {
      const base = COUNTRY_COLORS[s.country] || COUNTRY_COLORS.ot;

      if (id.kind === 'quarantine') {
        stayPoints.push({
          id: i === 0 ? id.key : undefined,
          name: id.name,
          start: s.from, end: s.to,
          color: QUAR_PATTERN,
          borderColor: '#C89B3C',
          custom: { identity: id, stay: s, quarantine: true }
        });
        return;
      }

      stayPoints.push({
        id: i === 0 ? id.key : undefined,   // définit le nœud parent
        name: id.name,                       // même name = même ligne
        start: s.from, end: s.to,
        color: s.confidence === 'f' ? faded(base) : base,
        borderColor: s.divergence ? '#B3403A' : 'rgba(0,0,0,.25)',
        borderWidth: s.divergence ? 2 : 1,
        custom: { identity: id, stay: s }
      });

      /* surcouche divergence : même ligne (même name), série séparée */
      if (s.divergence) {
        const d = s.divergence;
        divPoints.push({
          name: id.name,
          start: d.from, end: d.to,
          color: divergencePattern(COUNTRY_COLORS[d.country] || COUNTRY_COLORS.ot),
          borderColor: '#B3403A', borderWidth: 1.5,
          custom: { identity: id, stay: s, divergence: d }
        });
      }

      /* sous-lignes : une par logique, colorées par pays AFFIRMÉ
         (recommandation v4.1 : en cas de divergence, on voit quelle
         source affirme quel pays) */
      (s.contributions || []).forEach(c => {
        const cBase = COUNTRY_COLORS[c.country] || COUNTRY_COLORS.ot;
        c.intervals.forEach(iv => {
          contribPoints.push({
            parent: id.key,                 // enfant du nœud identité
            name: c.logic,                  // même logic = même sous-ligne
            start: iv.from,
            end: iv.point ? iv.from + 36e5 * 6 : iv.to,  // ponctuel : 6 h visibles
            milestone: !!iv.point,          // losange pour l'événement ponctuel
            color: faded(cBase),
            borderColor: cBase,
            custom: { identity: id, stay: s, logic: c.logic,
                      country: c.country, point: !!iv.point }
          });
        });
      });
    });
  });

  return [
    {
      name: 'שהויות',
      type: 'gantt',
      data: stayPoints,
      pointWidth: 16,
      zIndex: 2
    },
    {
      name: 'סתירה (מדינה שנייה)',
      type: 'gantt',
      data: divPoints,
      pointWidth: 9,          // plus étroit : lisible par-dessus l'hôte
      zIndex: 3,
      enableMouseTracking: true
    },
    {
      name: 'תרומות מקורות',
      type: 'gantt',
      data: contribPoints,
      pointWidth: 8,
      zIndex: 1,
      showInLegend: false
    }
  ];
}

/* ----------------------------------------------------------------
   3. Options du graphique
   ---------------------------------------------------------------- */
export function buildGanttOptions(identities, opts = {}) {
  const {
    searchStart, searchEnd,        // bornes de la recherche (ms UTC)
    indexStart,                    // début de couverture d'index
    rtl = true,                    // axe temporel inversé (à valider analystes)
    onStayClick = () => {}         // callback → flash de la ligne AG Grid
  } = opts;

  return {
    chart: {
      style: { fontFamily: "'IBM Plex Sans Hebrew', sans-serif" },
      spacing: [8, 10, 4, 4],
      zooming: { type: 'x' }        // glisser horizontal = zoom
    },
    title: { text: null },
    credits: { enabled: false },

    /* Gantt crée DEUX axes X liés (mois + jours). reversed doit
       être posé sur les deux pour le sens RTL. */
    xAxis: [
      {
        reversed: rtl,
        min: searchStart, max: searchEnd,
        currentDateIndicator: false,
        grid: { borderColor: '#D7DEE3' },
        plotBands: indexStart > searchStart ? [{
          from: searchStart, to: indexStart,
          color: '#E8EBED',
          label: {
            text: 'מחוץ לכיסוי האינדקס',
            style: { color: '#8A99A5', fontSize: '10px' }
          }
        }] : []
      },
      { reversed: rtl, grid: { borderColor: '#D7DEE3' } }
    ],

    yAxis: {
      type: 'treegrid',             // hiérarchie identité → logiques, repli natif
      uniqueNames: true,            // même name ⇒ même ligne (multi-barres)
      grid: { borderColor: '#EDF1F3' },
      labels: {
        align: 'right',             // labels côté lecture RTL
        style: { fontSize: '11px', color: '#16232E' },
        symbol: { type: 'triangle' }
      },
      staticScale: 34               // hauteur de ligne fixe → hauteur de chart auto
    },

    navigator: { enabled: true, series: { type: 'gantt' } },
    scrollbar: { enabled: true },
    rangeSelector: { enabled: false },   // vos presets de zoom restent côté Angular

    legend: { enabled: false },

    tooltip: {
      useHTML: true, outside: true,
      backgroundColor: '#16232E', borderWidth: 0,
      style: { color: '#E8EEF2', fontSize: '11px', direction: 'rtl' },
      formatter: function () {
        const c = this.point.options.custom || {};
        const fd = ts => Highcharts.dateFormat('%d/%m', ts);
        if (c.quarantine)
          return `<b>⚠ מזהה בהסגר</b><br>${c.identity.name}` +
                 `<br><span style="opacity:.7">מזהה «משותף» — מסלול שאינו בר־פרשנות</span>`;
        if (c.divergence) {
          const d = c.divergence, s = c.stay;
          return `<b>סתירה</b> — ${c.identity.name}` +
                 `<br>${s.countryName}: ודאות ${CONF_LABEL[s.confidence]} (${s.nSources} מקורות)` +
                 `<br>${d.countryName}: ודאות ${CONF_LABEL[d.confidence]} (${d.nSources})` +
                 `<br><span dir="ltr">${fd(d.from)} → ${fd(d.to - 864e5)}</span>` +
                 `<br><span style="opacity:.7">להכרעת האנליסט</span>`;
        }
        if (c.logic)
          return `<b>${c.logic}</b> — ${c.identity.name}` +
                 `<br><span dir="ltr">${fd(this.point.start)} → ${fd((this.point.end || this.point.start) - 864e5)}</span>` +
                 (c.point ? `<br><span style="opacity:.7">אירוע נקודתי</span>` : '');
        const s = c.stay;
        return `<b>${s.countryName}</b> — ${c.identity.name}` +
               `<br><span dir="ltr">${fd(s.from)} → ${fd(s.to - 864e5)}</span>` +
               ` · ${Math.round((s.to - s.from) / 864e5)} ימים` +
               `<br>ודאות ${CONF_LABEL[s.confidence]} · ${s.nSources} מקורות תומכים`;
      }
    },

    plotOptions: {
      gantt: {
        borderRadius: 2,
        states: { hover: { brightness: 0.08 } },
        point: {
          events: {
            click: function () {
              const c = this.options.custom || {};
              if (c.stay && !c.logic && !c.divergence)
                onStayClick(c.identity.key, c.stay.from);   // → grille
            }
          }
        }
      }
    },

    series: buildSeries(identities)
  };
}

/* ----------------------------------------------------------------
   4. Intégration Angular / AG Grid — points d'attention
   ----------------------------------------------------------------
   • Sélection grille → chart :
       chart.update({ series: buildSeries(selectedIdentities) },
                    true, true);   // oneToOne=true : lanes recréées
   • Chart → grille (onStayClick) :
       gridApi.ensureNodeVisible(node, 'middle');
       gridApi.flashCells({ rowNodes: [node] });
   • Hauteur : avec yAxis.staticScale, la hauteur suit le nombre
     de lignes visibles (repli compris) — pas de calcul manuel.
   • RTL : Highcharts n'a pas de mode RTL global. reversed sur les
     deux xAxis + labels alignés suffisent pour l'axe ; le conteneur
     Angular reste dir="rtl". AG Grid : enableRtl: true.
   • Le pattern-fill sur `color` accepte l'objet { pattern: … }
     uniquement si le module pattern-fill est chargé — sinon barre noire.
   • uniqueNames + id sur le premier séjour : si une identité n'a
     AUCUN séjour (résultat vide), créez un point fantôme
     { id, name, start:null } pour que la ligne existe quand même.
   ---------------------------------------------------------------- */
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
