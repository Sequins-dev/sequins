import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { init } from '../src/index';

describe('Sequins OTel SDK', () => {
  it('returns a handle with all three providers', async () => {
    const sequins = init({ serviceName: 'test-service' });

    assert.ok(sequins.tracerProvider, 'tracerProvider should be defined');
    assert.ok(sequins.meterProvider, 'meterProvider should be defined');
    assert.ok(sequins.loggerProvider, 'loggerProvider should be defined');
    assert.equal(typeof sequins.shutdown, 'function');

    await sequins.shutdown();
  });

  it('returns a tracer from tracerProvider', async () => {
    const sequins = init({ serviceName: 'test-service-2' });
    const tracer = sequins.tracerProvider.getTracer('test');
    assert.ok(tracer);
    await sequins.shutdown();
  });

  it('returns a meter from meterProvider', async () => {
    const sequins = init({ serviceName: 'test-service-3' });
    const meter = sequins.meterProvider.getMeter('test');
    assert.ok(meter);
    await sequins.shutdown();
  });

  it('shutdown resolves without error', async () => {
    const sequins = init({ serviceName: 'test-shutdown' });
    await assert.doesNotReject(() => sequins.shutdown());
  });
});
