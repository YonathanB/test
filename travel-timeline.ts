import type * as Highcharts from 'highcharts/highcharts-gantt';

// Toutes les dates sont des timestamps Unix en millisecondes.
export interface Presence {
  id: string;
  country_code: string; // Codes ISO 3 ; FR est aussi accepté pour la France.
  start_ts: number;
  end_ts: number;
  score: number; // Score normalisé entre 0 et 1.
}

export interface Travel {
  id: string;
  country_from: string;
  country_to: string;
  scheduled_from?: number | null;
  scheduled_to?: number | null;
  confirmed_from?: number | null;
  confirmed_to?: number | null;
}

const COLORS: Record<string, string> = {
  FRA: '#2563eb', DEU: '#16a34a', ESP: '#ea580c',
  BEL: '#9333ea', THA: '#db2777', USA: '#0891b2'
};
const ALIASES: Record<string, string> = {
  FR: 'FRA', DE: 'DEU', ES: 'ESP', BE: 'BEL', TH: 'THA', US: 'USA'
};
const canonical = (code: string): string => {
  const value = code.trim().toUpperCase();
  return ALIASES[value] ?? value;
};
const row = (country: string): number => canonical(country) === 'FRA' ? 0 : 1;
const isTime = (value: unknown): value is number =>
  typeof value === 'number' && Number.isFinite(value);

function countryColor(country: string, opacity: number): string {
  const code = canonical(country);
  // Couleur stable, indépendante de l'ordre des données reçues.
  let hash = 0;
  for (const c of code) hash = (hash * 31 + c.charCodeAt(0)) >>> 0;
  const fallback = [60 + hash % 150, 60 + (hash >>> 8) % 150, 60 + (hash >>> 16) % 150]
    .map(value => value.toString(16).padStart(2, '0')).join('');
  const hex = (COLORS[code] ?? `#${fallback}`).slice(1);
  const rgb = [0, 2, 4].map(offset => parseInt(hex.slice(offset, offset + 2), 16));
  return `rgba(${rgb.join(',')},${opacity})`;
}

// Une couche par graphique : aucun cumul de dessins après zoom/redimensionnement.
const layers = new WeakMap<Highcharts.Chart, {
  group: Highcharts.SVGElement;
  clip: Highcharts.SVGElement;
}>();

function drawTravels(chart: Highcharts.Chart, travels: readonly Travel[]): void {
  const previous = layers.get(chart);
  previous?.group.destroy();
  previous?.clip.destroy();

  const renderer = chart.renderer;
  const clip = renderer.clipRect(0, 0, chart.plotWidth, chart.plotHeight);
  const group = renderer.g('travel-overlays')
    .attr({ translateX: chart.plotLeft, translateY: chart.plotTop, zIndex: 7 })
    .css({ pointerEvents: 'none' }).add().clip(clip);
  layers.set(chart, { group, clip });

  const x = (time: number): number => chart.xAxis[0].toPixels(time, true);
  const y = (country: string): number => chart.yAxis[0].toPixels(row(country), true);

  for (const travel of travels) {
    // Flèche prévue. Son décalage vertical laisse les libellés des présences lisibles.
    if (isTime(travel.scheduled_from) && isTime(travel.scheduled_to)) {
      const x1 = x(travel.scheduled_from), y1 = y(travel.country_from) - 22;
      const x2 = x(travel.scheduled_to), y2 = y(travel.country_to) - 22;
      const length = Math.hypot(x2 - x1, y2 - y1);

      if (length > 0.1) {
        const ux = (x2 - x1) / length, uy = (y2 - y1) / length;
        const size = Math.min(8, length / 2), halfWidth = size / 2;
        renderer.path([['M', x1, y1], ['L', x2, y2]])
          .attr({ stroke: '#334155', 'stroke-width': 2, dashstyle: 'ShortDash' })
          .addClass('travel-scheduled-line').add(group);
        renderer.path([
          ['M', x2, y2],
          ['L', x2 - size * ux + halfWidth * uy, y2 - size * uy - halfWidth * ux],
          ['L', x2 - size * ux - halfWidth * uy, y2 - size * uy + halfWidth * ux],
          ['Z']
        ]).attr({ fill: '#334155' }).addClass('travel-arrowhead').add(group);
      } else {
        // Même instant et même ligne : les deux extrémités se confondent.
        renderer.circle(x1, y1, 3).attr({ fill: '#334155' }).add(group);
      }
    }

    // Chaque confirmation est indépendante : une seule date suffit à tracer son repère.
    const confirmations = [
      { time: travel.confirmed_from, country: travel.country_from, kind: 'from' },
      { time: travel.confirmed_to, country: travel.country_to, kind: 'to' }
    ];
    for (const confirmation of confirmations) {
      if (!isTime(confirmation.time)) continue;
      const px = x(confirmation.time), py = y(confirmation.country);
      renderer.path([['M', px, py - 26], ['L', px, py + 20]])
        .attr({ stroke: '#0f172a', 'stroke-width': 3 })
        .addClass(`travel-confirmed-${confirmation.kind}`).add(group);
    }
  }
}

/**
 * Options utilisables avec Highcharts.ganttChart ou le wrapper highcharts-angular.
 * Recréer ces options quand les données changent ; les tableaux ne sont pas modifiés.
 * Le module Gantt doit être chargé dans l'instance Highcharts de l'application.
 */
export function buildTravelTimelineOptions(
  presences: readonly Presence[],
  travels: readonly Travel[],
  timezone = 'UTC'
): Highcharts.Options {
  let min = Infinity, max = -Infinity;
  const include = (time: unknown): void => {
    if (isTime(time)) { min = Math.min(min, time); max = Math.max(max, time); }
  };

  const data: Highcharts.GanttPointOptionsObject[] = presences.map(presence => {
    if (!isTime(presence.start_ts) || !isTime(presence.end_ts) ||
        presence.end_ts < presence.start_ts || !Number.isFinite(presence.score)) {
      throw new Error(`Présence invalide : ${presence.id}`);
    }
    include(presence.start_ts);
    include(presence.end_ts);
    const score = Math.max(0, Math.min(1, presence.score));
    const opacity = 0.2 + 0.8 * score; // Plancher pour garder les petits scores visibles.
    return {
      id: presence.id,
      name: canonical(presence.country_code),
      start: presence.start_ts,
      end: presence.end_ts,
      y: row(presence.country_code),
      color: countryColor(presence.country_code, opacity),
      // Opacité du remplissage uniquement : le texte reste lisible.
      custom: { scorePercent: Math.round(score * 100) },
      borderWidth: 0
    };
  });

  for (const travel of travels) {
    if (isTime(travel.scheduled_from) && isTime(travel.scheduled_to) &&
        travel.scheduled_to < travel.scheduled_from) {
      throw new Error(`Dates prévues inversées : ${travel.id}`);
    }
    [travel.scheduled_from, travel.scheduled_to,
      travel.confirmed_from, travel.confirmed_to].forEach(include);
  }

  const padding = Math.max(Number.isFinite(min) ? (max - min) * 0.03 : 0, 60_000);
  return {
    chart: {
      height: 330, animation: false, styledMode: false,
      zooming: { type: 'x' },
      events: {
        render: function () {
          drawTravels(this, travels);
        }
      }
    },
    title: { text: undefined },
    time: { timezone },
    credits: { enabled: false },
    navigator: { enabled: false },
    scrollbar: { enabled: false },
    rangeSelector: { enabled: false },
    legend: { enabled: false },
    xAxis: {
      type: 'datetime', minRange: 60_000,
      min: Number.isFinite(min) ? min - padding : undefined,
      max: Number.isFinite(max) ? max + padding : undefined,
      startOnTick: false, endOnTick: false,
      labels: { format: '{value:%d/%m %H:%M}' }
    },
    yAxis: {
      type: 'category', categories: ['France', 'Étranger'],
      reversed: true, min: 0, max: 1,
      startOnTick: false, endOnTick: false,
      tickPositions: [0, 1], title: { text: undefined },
      grid: { enabled: true }
    },
    tooltip: {
      headerFormat: '',
      pointFormat: '<b>{point.name}</b><br/>' +
        '{point.start:%d/%m %H:%M} → {point.end:%d/%m %H:%M}<br/>' +
        'Score : {point.custom.scorePercent}%'
    },
    plotOptions: {
      gantt: {
        animation: false, grouping: false, pointWidth: 26, minPointLength: 3,
        colorByPoint: false, borderRadius: 3,
        dataLabels: {
          enabled: true, format: '{point.name}',
          style: { color: '#0f172a', textOutline: 'none', fontWeight: '600' }
        },
        states: { inactive: { opacity: 1 }, hover: { brightness: 0 } }
      }
    },
    series: [{ id: 'presences', type: 'gantt', name: 'Présences', data }]
  };
}

// Exemple : timestamps UTC explicites. Adapter le fuseau à celui de ton API.
const ts = (value: string): number => Date.parse(value);
export const EXAMPLE_PRESENCES: Presence[] = [
  { id: 'p1', country_code: 'FRA', start_ts: ts('2026-05-07T09:00:00Z'),
    end_ts: ts('2026-05-07T13:00:00Z'), score: 0.95 },
  { id: 'p2', country_code: 'ESP', start_ts: ts('2026-05-07T15:10:00Z'),
    end_ts: ts('2026-05-09T00:40:00Z'), score: 0.70 },
  { id: 'p3', country_code: 'FRA', start_ts: ts('2026-05-09T02:45:00Z'),
    end_ts: ts('2026-05-09T16:00:00Z'), score: 0.90 }
];
export const EXAMPLE_TRAVELS: Travel[] = [
  { id: 't1', country_from: 'FRA', country_to: 'ESP',
    scheduled_from: ts('2026-05-07T12:45:00Z'), scheduled_to: ts('2026-05-07T15:00:00Z'),
    confirmed_from: ts('2026-05-07T13:00:00Z'), confirmed_to: ts('2026-05-07T15:10:00Z') },
  { id: 't2', country_from: 'ESP', country_to: 'FRA',
    scheduled_from: ts('2026-05-09T00:30:00Z'), scheduled_to: ts('2026-05-09T02:30:00Z'),
    confirmed_from: ts('2026-05-09T00:40:00Z'), confirmed_to: ts('2026-05-09T02:45:00Z') }
];
