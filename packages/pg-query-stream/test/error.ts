/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

import assert from 'assert'
import helper from './helper'
import QueryStream from '../src'
import { Pool, Client } from 'pg'

helper('error', function (client) {
  it('receives error on stream', function (done) {
    const stream = new QueryStream('SELECT * FROM asdf num', [])
    const query = client.query(stream)
    query
      .on('error', function (err) {
        assert(err)
        assert.equal(err.code, '42P01')
        done()
      })
      .on('data', function () {
        // noop to kick of reading
      })
  })

  it('continues to function after stream', function (done) {
    client.query('SELECT NOW()', done)
  })
})

describe('error recovery', () => {
  // created from https://github.com/chrisdickinson/pg-test-case
  it('recovers from a streaming error in a transaction', async () => {
    const pool = new Pool()
    const client = await pool.connect()
    await client.query(`CREATE TEMP TABLE frobnicators (
      id serial primary key,
      updated timestamp
    )`)
    await client.query(`BEGIN;`)
    const query = new QueryStream(`INSERT INTO frobnicators ("updated") VALUES ($1) RETURNING "id"`, [Date.now()])
    let error: Error | undefined = undefined
    query.on('data', console.log).on('error', (e) => {
      error = e
    })
    client.query(query) // useless callback necessitated by an older version of honeycomb-beeline

    await client.query(`ROLLBACK`)
    assert(error, 'Error should not be undefined')
    const { rows } = await client.query('SELECT NOW()')
    assert.strictEqual(rows.length, 1)
    client.release()
    const client2 = await pool.connect()
    await client2.query(`BEGIN`)
    client2.release()
    pool.end()
  })

  // created from https://github.com/brianc/node-postgres/pull/2333
  it('handles an error on a stream after a plain text non-stream error', async () => {
    const client = new Client()
    const stmt = 'SELECT * FROM goose;'
    await client.connect()
    return new Promise((resolve, reject) => {
      client.query(stmt).catch((e) => {
        assert(e, 'Query should have rejected with an error')
        const stream = new QueryStream('SELECT * FROM duck')
        client.query(stream)
        stream.on('data', () => {})
        stream.on('error', () => {
          client.end((err) => {
            err ? reject(err) : resolve()
          })
        })
      })
    })
  })

  it('does not crash when closing a connection with a queued stream', async () => {
    const client = new Client()
    const stmt = 'SELECT * FROM goose;'
    await client.connect()
    return new Promise(async (resolve) => {
      let queryError: Error | undefined
      client.query(stmt).catch((e) => {
        queryError = e
      })
      const stream = client.query(new QueryStream(stmt))
      stream.on('data', () => {})
      stream.on('error', () => {
        assert(queryError, 'query should have errored due to client ending')
        resolve()
      })
      await client.end()
    })
  })
})
