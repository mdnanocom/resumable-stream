import { describe, it, expect } from "vitest";
import { createResumableStreamContext } from "../generic";
import { createInMemoryPubSubForTesting } from "../../testing-utils/in-memory-pubsub";
import { streamToBuffer, createTestingStream } from "../../testing-utils/testing-stream";

describe("generic interface", () => {
  it("should work with custom publisher/subscriber implementations", async () => {
    const { publisher, subscriber } = createInMemoryPubSubForTesting();

    const ctx = createResumableStreamContext({
      waitUntil: null,
      publisher,
      subscriber,
      keyPrefix: "test-generic-" + crypto.randomUUID(),
    });

    const { readable, writer } = createTestingStream();
    const stream = await ctx.resumableStream("test-stream", () => readable);

    writer.write("Hello ");
    writer.write("World!");
    writer.close();

    expect(stream).not.toBeNull();
    const result = await streamToBuffer(stream!);
    expect(result).toBe("Hello World!");
  });

  it("should resume streams with custom implementations", async () => {
    const { publisher, subscriber } = createInMemoryPubSubForTesting();

    const ctx = createResumableStreamContext({
      waitUntil: null,
      publisher,
      subscriber,
      keyPrefix: "test-generic-" + crypto.randomUUID(),
    });

    const { readable, writer } = createTestingStream();
    // Create initial stream
    const stream1 = await ctx.resumableStream("test-stream-2", () => readable);

    // Resume the same stream immediately
    const stream2 = await ctx.resumableStream("test-stream-2", () => {
      throw new Error("Should not be called");
    });

    writer.write("Part 1 ");
    writer.write("Part 2");
    writer.close();

    expect(stream1).not.toBeNull();
    expect(stream2).not.toBeNull();

    const result1 = await streamToBuffer(stream1!);
    const result2 = await streamToBuffer(stream2!);
    expect(result1).toBe("Part 1 Part 2");
    expect(result2).toBe("Part 1 Part 2");
  });

  it("should return null if stream is done", async () => {
    const { publisher, subscriber } = createInMemoryPubSubForTesting();

    const ctx = createResumableStreamContext({
      waitUntil: null,
      publisher,
      subscriber,
      keyPrefix: "test-generic-" + crypto.randomUUID(),
    });

    const { readable, writer } = createTestingStream();
    const stream = await ctx.resumableStream("test-stream-3", () => readable);

    writer.write("Done");
    writer.close();

    await streamToBuffer(stream!);

    // Try to resume after stream is done
    const doneStream = await ctx.resumableStream("test-stream-3", () => {
      throw new Error("Should not be called");
    });

    expect(doneStream).toBeNull();
  });

  it("should throw error if publisher is not provided", () => {
    expect(() => {
      createResumableStreamContext({
        waitUntil: null,
        // @ts-expect-error - intentionally not providing publisher/subscriber to test error
      });
    }).toThrow();
  });

  it("should use the default ack timeout", async () => {
    const keyPrefix = "test-generic-" + crypto.randomUUID();
    const { publisher, subscriber } = createInMemoryPubSubForTesting({
      deliveryDelayMs: (channel) => (channel.startsWith(`${keyPrefix}:rs:chunk:`) ? 1_100 : 0),
    });

    const ctx = createResumableStreamContext({
      waitUntil: null,
      publisher,
      subscriber,
      keyPrefix,
    });

    const { readable, writer } = createTestingStream();
    const stream = await ctx.resumableStream("test-stream-ack-default", () => readable);

    await expect(
      ctx.resumableStream("test-stream-ack-default", () => {
        throw new Error("Should not be called");
      })
    ).rejects.toThrow("Timeout waiting for ack");

    writer.close();
    await streamToBuffer(stream!);
  });

  it("should allow a custom ack timeout", async () => {
    const keyPrefix = "test-generic-" + crypto.randomUUID();
    const { publisher, subscriber } = createInMemoryPubSubForTesting({
      deliveryDelayMs: (channel) => (channel.startsWith(`${keyPrefix}:rs:chunk:`) ? 1_100 : 0),
    });

    const ctx = createResumableStreamContext({
      waitUntil: null,
      publisher,
      subscriber,
      keyPrefix,
      ackTimeoutMs: 1_500,
    });

    const { readable, writer } = createTestingStream();
    const stream1 = await ctx.resumableStream("test-stream-ack-custom", () => readable);
    const stream2 = await ctx.resumableStream("test-stream-ack-custom", () => {
      throw new Error("Should not be called");
    });

    writer.write("Hello");
    writer.close();

    expect(await streamToBuffer(stream1!)).toBe("Hello");
    expect(await streamToBuffer(stream2!)).toBe("Hello");
  });

  it("should validate ackTimeoutMs", () => {
    for (const ackTimeoutMs of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() => {
        createResumableStreamContext({
          waitUntil: null,
          publisher: createInMemoryPubSubForTesting().publisher,
          subscriber: createInMemoryPubSubForTesting().subscriber,
          ackTimeoutMs,
        });
      }).toThrow("ackTimeoutMs must be a positive finite number");
    }
  });
});
