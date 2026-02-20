// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { classifyNiFiProcessor } from '../src/mappers/processor-classifier.js';

describe('classifyNiFiProcessor', () => {
  it('should classify known source processors', () => {
    expect(classifyNiFiProcessor('GetFile')).toBe('source');
    expect(classifyNiFiProcessor('ConsumeKafka')).toBe('source');
    expect(classifyNiFiProcessor('ListS3')).toBe('source');
    expect(classifyNiFiProcessor('QueryDatabaseTable')).toBe('source');
  });

  it('should classify known transform processors', () => {
    expect(classifyNiFiProcessor('ReplaceText')).toBe('transform');
    expect(classifyNiFiProcessor('EvaluateJsonPath')).toBe('transform');
    expect(classifyNiFiProcessor('ConvertRecord')).toBe('transform');
    expect(classifyNiFiProcessor('JoltTransformJSON')).toBe('transform');
  });

  it('should classify known sink processors', () => {
    expect(classifyNiFiProcessor('PutFile')).toBe('sink');
    expect(classifyNiFiProcessor('PutS3Object')).toBe('sink');
    expect(classifyNiFiProcessor('PublishKafka')).toBe('sink');
    expect(classifyNiFiProcessor('PutDatabaseRecord')).toBe('sink');
  });

  it('should fall back to prefix-based classification for unknown types', () => {
    expect(classifyNiFiProcessor('GetCustomData')).toBe('source');
    expect(classifyNiFiProcessor('PutCustomSink')).toBe('sink');
    expect(classifyNiFiProcessor('ConvertCustomFormat')).toBe('transform');
    expect(classifyNiFiProcessor('RouteOnCustom')).toBe('route');
    expect(classifyNiFiProcessor('ExecuteCustomScript')).toBe('process');
  });

  it('should return "process" for completely unknown processor types', () => {
    expect(classifyNiFiProcessor('MyCustomProcessor')).toBe('process');
    expect(classifyNiFiProcessor('DataEnricher')).toBe('process');
  });
});
