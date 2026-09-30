type RejectionHandler = (error: unknown) => void;

const handlers = new Set<RejectionHandler>();

function dispatch(error: unknown): void {
  for (const handler of handlers) handler(error);
}

/** One process listener however many hosts run, so a rejection is reported without piling up listeners. */
export function guardUnhandledRejections(
  handler: RejectionHandler,
): () => void {
  if (!handlers.size) process.on("unhandledRejection", dispatch);
  handlers.add(handler);
  return () => {
    handlers.delete(handler);
    if (!handlers.size) process.off("unhandledRejection", dispatch);
  };
}
