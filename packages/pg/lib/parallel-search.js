'use strict'

const Client = require('./client');
const pLimit = require('p-limit');

class ParallelSearch {
  constructor() {
    this.connections = [];
    this.connectionIndex = 0;
  }

  async executeMultiSearch(dbConfig, sqlTemplate, paramsList, searchParams, maxThreads) {
    if (!dbConfig) throw new Error('missing database configuration');
    try {
            this.validateSqlTemplate(sqlTemplate);
        } catch (error) {
            throw new Error(`SQL template validation failed: ${error.message}`);
    }
    if (!Array.isArray(paramsList) || !paramsList.length) throw new Error('invalid paramsList');

    if (maxThreads <= 0) throw new Error('number of threads must be greater than 0');
    
    maxThreads = Math.min(maxThreads, paramsList.length);
    try {
      await this.initConnections(dbConfig, maxThreads, searchParams);

      const limit = pLimit(maxThreads);

      const queryTasks = paramsList.map((params, index) => 
        limit(async () => {
          const client = this.getNextConnection();
          try {
            const result = await client.query(sqlTemplate, params);
            return {
              success: true,
              fields: result.fields.map(field => field.name),
              data: result.rows,
              index
            };
          } catch (error) {
            return {
              success: false,
              fields: [],
              data: [],
              error: error.message,
              index
            };
          }
        })
      );

      const results = await Promise.all(queryTasks);
      return results;
    } finally {
      await this.closeConnections();
    }
  }

  validateSqlTemplate(sqlTemplate) {
    if (typeof sqlTemplate !== 'string' || !sqlTemplate.trim()) {
        throw new Error('invalid sql template: must be a non-empty string');
    }

    const trimmedSql = sqlTemplate.trim();

    const selectRegex = /^\s*(?:--.*\s*)*SELECT\s+/i;
    if (!selectRegex.test(trimmedSql)) {
        throw new Error('invalid sql template: must be a SELECT statement');
    }

    const sqlWithoutComments = trimmedSql.replace(/--.*$/gm, '').trim();
    const statements = sqlWithoutComments.split(';')
        .map(s => s.trim())
        .filter(s => s.length > 0);
    
    if (statements.length !== 1) {
        throw new Error(`must contain exactly one query, found ${statements.length}`);
    }

    const vectorOperators = /<->|<=>|<#>|<\+>|<~>|<%>/;
    if (!vectorOperators.test(trimmedSql)) {
        throw new Error('invalid sql template: must contain vector operator <->, <=>, <+>, <~>, <%> or <#>');
    }
  }

  async initConnections(dbConfig, maxThreads, searchParams) {
    const setStatements = this.buildSetStatements(searchParams);
    
    const connectionPromises = Array.from({ length: maxThreads }, async () => {
      const client = new Client(dbConfig);
      await client.connect();
      if (setStatements) {
        await client.query(setStatements);
      }
      return client;
    });

    this.connections = await Promise.all(connectionPromises);
  }

  getNextConnection() {
    const client = this.connections[this.connectionIndex];
    this.connectionIndex = (this.connectionIndex + 1) % this.connections.length;
    return client;
  }

  async closeConnections() {
    if (this.connections.length === 0) return;

    await Promise.all(
      this.connections.map(client => client.end().catch(() => {}))
    );
    this.connections = [];
    this.connectionIndex = 0;
  }

  buildSetStatements(searchParams) {
    if (!searchParams || typeof searchParams !== 'object' || Object.keys(searchParams).length === 0) {
      return null;
    }

    return Object.entries(searchParams)
      .map(([key, value]) => {
        const formattedValue = typeof value === 'string' ? `'${value}'` : String(value);
        return `set ${key}=${formattedValue};`;
      })
      .join(' ');
  }
}

module.exports = ParallelSearch;