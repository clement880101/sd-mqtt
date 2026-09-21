/**
 * A line chart, which is uPlot with a lifecycle around it.
 *
 * uPlot is imperative and owns its own DOM, so the component's whole
 * job is to create it once, hand it new data when the data changes,
 * tell it when the box it is in has resized, and destroy it on the
 * way out. None of that belongs in a page.
 *
 * **Pinned to the published version.** uPlot's repository is much
 * newer than what is on the registry, and a dependency taken from a
 * commit has no version for anybody to audit, no tag to compare
 * against and nothing an advisory database can name. About 23 KB
 * gzipped against the 150 KB of section 15.
 *
 * # Colours come from the stylesheet, once
 *
 * uPlot wants concrete colours, not custom properties, so they are
 * read from the host element's computed style when the plot is
 * created. The consequence is deliberate and worth knowing: a
 * *change* of colour scheme while the page is open does not restyle
 * an existing plot. Watching for that would mean a media query
 * listener per chart to redraw something an operator sees once, and
 * a reload fixes it.
 */
import uPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';
import { useEffect, useRef } from 'preact/hooks';

/** One series of a chart. */
export interface Line {
  /** What the legend calls it. */
  label: string;
  /** Its colour, as a custom property on the host element. */
  colour: string;
}

/** What a chart is given. */
export interface ChartProps {
  /** For the legend above the plot. */
  title: string;
  /** uPlot's own layout: `[x, ...series]`, a `null` being a gap. */
  data: uPlot.AlignedData;
  /** One entry per series after the x values. */
  lines: Line[];
  /** How tall the plot is, in pixels. */
  height?: number;
  /** How a value is written in the legend, where there is room. */
  format: (value: number) => string;
  /**
   * How a value is written on the axis, where there is not.
   *
   * A separate formatter because the axis is a fixed-width gutter
   * and uPlot clips rather than widening it: "334.4 kB/s" written
   * there reads as "0.0 kB/s", which is not a smaller number, it is
   * a wrong one.
   */
  formatTick: (value: number) => string;
}

/** Read a custom property off an element, with a colour to fall back on. */
function property(element: HTMLElement, name: string, fallback: string): string {
  const value = getComputedStyle(element).getPropertyValue(name).trim();
  return value || fallback;
}

/**
 * A line chart over live data.
 *
 * The plot is created once and fed afterwards. Recreating it on
 * every sample would throw away the axis scales and the cursor a
 * person is reading with, once a second.
 */
export function Chart({ title, data, lines, height = 170, format, formatTick }: ChartProps) {
  const host = useRef<HTMLDivElement>(null);
  const plot = useRef<uPlot | null>(null);
  // Read inside the creation effect without making it depend on the
  // data, which changes every second.
  const latest = useRef(data);
  latest.current = data;

  useEffect(() => {
    const element = host.current;
    if (!element) {
      return;
    }
    const ink = property(element, '--ink-quiet', '#888');
    const edge = property(element, '--edge', '#ccc');
    const axis = {
      stroke: ink,
      grid: { stroke: edge, width: 1 },
      ticks: { stroke: edge, width: 1 },
    };
    const created = new uPlot(
      {
        title,
        width: element.clientWidth || 600,
        height,
        // The legend is the value readout beside the cursor. Kept,
        // because a rate chart with no numbers on it is a shape.
        legend: { live: true },
        cursor: { y: false },
        scales: { x: { time: true } },
        axes: [
          { ...axis, space: 80 },
          { ...axis, size: 56, values: (_plot, ticks) => ticks.map(formatTick) },
        ],
        series: [
          { label: 'time' },
          ...lines.map((line) => ({
            label: line.label,
            stroke: property(element, line.colour, '#4c6ef5'),
            width: 1.5,
            // A `null` is a gap and must read as one: see `live.ts`
            // on what a broker restart draws.
            spanGaps: false,
            value: (_plot: uPlot, value: number | null) => (value === null ? '—' : format(value)),
          })),
        ],
      },
      latest.current,
      element,
    );
    plot.current = created;

    // The plot is a fixed-size canvas, so it has to be told.
    const observer = new ResizeObserver(() => {
      created.setSize({ width: element.clientWidth, height });
    });
    observer.observe(element);

    return () => {
      observer.disconnect();
      created.destroy();
      plot.current = null;
    };
    // Created once for a given shape. `data` is deliberately absent:
    // it changes once a second and is pushed in below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [title, height, lines, format, formatTick]);

  useEffect(() => {
    plot.current?.setData(data);
  }, [data]);

  return <div class="chart" ref={host} />;
}
