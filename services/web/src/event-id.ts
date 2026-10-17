/**
 * The part of an event identifier worth showing.
 *
 * The broker sends `<run>-<number>`: the run is when the process
 * started, and is how it tells a browser that reconnects after a
 * restart to start over. An operator reading the status line wants the
 * number. A bare number, which an older broker sends, is shown as it
 * is.
 */
export function eventNumber(id: string | null): string | null {
  if (!id) return null;
  return id.slice(id.lastIndexOf('-') + 1);
}
