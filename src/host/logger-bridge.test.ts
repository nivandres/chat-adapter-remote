import type { Logger } from "chat";
import { describe, expect, it, vi } from "vitest";

import { createBridgingLogger } from "./logger-bridge";

function localLogger(): Logger {
  const logger = {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    child: vi.fn(),
  };
  logger.child.mockReturnValue(logger);
  return logger as unknown as Logger;
}

describe("logger bridge", () => {
  it("always logs locally and forwards at or above the threshold", () => {
    const local = localLogger();
    const notify = vi.fn();
    const logger = createBridgingLogger({
      localLogger: local,
      notify,
      prefix: "baileys",
    });

    logger.debug("noisy");
    logger.info("useful", { a: 1 });

    expect(local.debug).toHaveBeenCalledWith("noisy");
    expect(notify).toHaveBeenCalledTimes(1);
    expect(notify).toHaveBeenCalledWith("info", "baileys", "useful", [
      { a: 1 },
    ]);
  });

  it("honours a raised threshold", () => {
    const notify = vi.fn();
    const logger = createBridgingLogger({
      localLogger: localLogger(),
      notify,
      forwardLevel: "error",
    });

    logger.warn("ignored");
    logger.error("kept");

    expect(notify).toHaveBeenCalledTimes(1);
    expect(notify).toHaveBeenCalledWith("error", "", "kept", []);
  });

  it("keeps the threshold and notifier across child loggers", () => {
    const notify = vi.fn();
    const logger = createBridgingLogger({
      localLogger: localLogger(),
      notify,
      forwardLevel: "error",
    });

    logger.child("socket").warn("ignored");
    logger.child("socket").error("kept");

    expect(notify).toHaveBeenCalledTimes(1);
    expect(notify).toHaveBeenCalledWith("error", "socket", "kept", []);
  });
});
