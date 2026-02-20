// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { detectExternalSystems } from '../src/analyzers/external-systems.js';

describe('detectExternalSystems', () => {
  it('should detect Kafka from processor types', () => {
    const nifi = {
      processors: [
        { name: 'Read Kafka', type: 'ConsumeKafka_2_6', group: 'root' },
        { name: 'Write Kafka', type: 'PublishKafka_2_6', group: 'root' },
      ],
    };
    const systems = detectExternalSystems(nifi);
    expect(systems.kafka).toBeDefined();
    expect(systems.kafka.processors).toHaveLength(2);
    expect(systems.kafka.name).toBe('Apache Kafka');
    expect(systems.kafka.packages).toContain('confluent-kafka');
  });

  it('should detect JDBC from deep property inventory', () => {
    const nifi = {
      processors: [],
      deepPropertyInventory: {
        jdbcUrls: {
          'jdbc:oracle:thin:@host:1521:db': true,
          'jdbc:mysql://host:3306/mydb': true,
        },
      },
    };
    const systems = detectExternalSystems(nifi);
    expect(systems.oracle).toBeDefined();
    expect(systems.oracle.jdbcUrls).toHaveLength(1);
    expect(systems.mysql).toBeDefined();
    expect(systems.mysql.jdbcUrls).toHaveLength(1);
  });

  it('should detect S3/cloud from processor types', () => {
    const nifi = {
      processors: [
        { name: 'List Bucket', type: 'ListS3', group: 'root' },
        { name: 'Upload', type: 'PutS3Object', group: 'root' },
      ],
    };
    const systems = detectExternalSystems(nifi);
    expect(systems.s3).toBeDefined();
    expect(systems.s3.processors).toHaveLength(2);
    expect(systems.s3.dbxApproach).toContain('Pre-installed');
  });

  it('should return empty object when no systems are found', () => {
    const nifi = {
      processors: [
        { name: 'Custom Proc', type: 'MyCustomProcessor', group: 'root' },
      ],
    };
    const systems = detectExternalSystems(nifi);
    expect(Object.keys(systems)).toHaveLength(0);
  });
});
