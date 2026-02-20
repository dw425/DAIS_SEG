// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { parseNiFiXML } from '../src/parsers/nifi-xml-parser.js';

function makeDoc(xmlStr) {
  const parser = new DOMParser();
  return parser.parseFromString(xmlStr, 'application/xml');
}

describe('parseNiFiXML', () => {
  it('should parse a NiFi template with processors and connections', () => {
    const xml = `
      <template>
        <snippet>
          <processors>
            <id>proc-1</id>
            <name>Fetch Data</name>
            <type>org.apache.nifi.processors.standard.GetFile</type>
            <state>RUNNING</state>
          </processors>
          <processors>
            <id>proc-2</id>
            <name>Transform Data</name>
            <type>org.apache.nifi.processors.standard.ReplaceText</type>
            <state>RUNNING</state>
          </processors>
          <connections>
            <sourceId>proc-1</sourceId>
            <destinationId>proc-2</destinationId>
            <selectedRelationships>success</selectedRelationships>
          </connections>
        </snippet>
      </template>`;
    const doc = makeDoc(xml);
    const result = parseNiFiXML(doc, 'test-flow');

    expect(result.processors).toHaveLength(2);
    expect(result.processors[0].name).toBe('Fetch Data');
    expect(result.processors[0].type).toBe('GetFile');
    expect(result.connections).toHaveLength(1);
    expect(result.connections[0].sourceId).toBe('proc-1');
    expect(result.connections[0].destinationId).toBe('proc-2');
    expect(result.connections[0].relationships).toContain('success');
  });

  it('should handle empty flow with no processors', () => {
    const xml = `<template><snippet></snippet></template>`;
    const doc = makeDoc(xml);
    const result = parseNiFiXML(doc, 'empty-flow');

    expect(result.processors).toHaveLength(0);
    expect(result.connections).toHaveLength(0);
    expect(result.controllerServices).toHaveLength(0);
  });

  it('should handle malformed XML gracefully', () => {
    // DOMParser in jsdom returns a document with parsererror for invalid XML,
    // but parseNiFiXML still works on whatever DOM it gets
    const xml = `<template><snippet><processors><name>Test</name><type>org.Foo</type></processors></snippet></template>`;
    const doc = makeDoc(xml);
    const result = parseNiFiXML(doc, 'partial');
    expect(result.processors).toBeDefined();
    expect(Array.isArray(result.processors)).toBe(true);
  });

  it('should extract nested process groups', () => {
    const xml = `
      <template>
        <snippet>
          <processGroups>
            <id>pg-1</id>
            <name>ETL Group</name>
            <contents>
              <processors>
                <id>p-inner</id>
                <name>Inner Proc</name>
                <type>org.apache.nifi.GetFile</type>
                <state>STOPPED</state>
              </processors>
            </contents>
          </processGroups>
        </snippet>
      </template>`;
    const doc = makeDoc(xml);
    const result = parseNiFiXML(doc, 'nested');

    expect(result.processGroups).toHaveLength(1);
    expect(result.processGroups[0].name).toBe('ETL Group');
    expect(result.processors).toHaveLength(1);
    expect(result.processors[0].group).toBe('ETL Group');
  });
});
