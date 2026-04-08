<?php

declare(strict_types=1);

namespace Sequins\Otel\Tests;

use PHPUnit\Framework\TestCase;
use Sequins\Otel\Sequins;
use Sequins\Otel\SequinsHandle;

class SequinsTest extends TestCase
{
    public function testInitReturnsHandle(): void
    {
        $handle = Sequins::init('test-service');
        $this->assertInstanceOf(SequinsHandle::class, $handle);
        $handle->shutdown();
    }

    public function testTracerProviderIsNotNull(): void
    {
        $handle = Sequins::init('test-tracer');
        $this->assertNotNull($handle->getTracerProvider());
        $handle->shutdown();
    }

    public function testMeterProviderIsNotNull(): void
    {
        $handle = Sequins::init('test-meter');
        $this->assertNotNull($handle->getMeterProvider());
        $handle->shutdown();
    }

    public function testLoggerProviderIsNotNull(): void
    {
        $handle = Sequins::init('test-logger');
        $this->assertNotNull($handle->getLoggerProvider());
        $handle->shutdown();
    }

    public function testCanGetTracerFromProvider(): void
    {
        $handle = Sequins::init('test-get-tracer');
        $tracer = $handle->getTracerProvider()->getTracer('my-module');
        $this->assertNotNull($tracer);
        $handle->shutdown();
    }

    public function testShutdownDoesNotThrow(): void
    {
        $handle = Sequins::init('test-shutdown');
        $this->expectNotToPerformAssertions();
        $handle->shutdown();
    }
}
