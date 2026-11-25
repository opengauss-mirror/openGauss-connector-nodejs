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

const assert = require('assert')
const Cursor = require('../')
const pg = require('pg')

describe('transactions', () => {
  it('can execute multiple statements in a transaction', async () => {
    const client = new pg.Client()
    await client.connect()
    await client.query('begin')
    await client.query('CREATE TEMP TABLE foobar(id SERIAL PRIMARY KEY)')
    const cursor = client.query(new Cursor('SELECT * FROM foobar'))
    const rows = await new Promise((resolve, reject) => {
      cursor.read(10, (err, rows) => (err ? reject(err) : resolve(rows)))
    })
    assert.strictEqual(rows.length, 0)
    await client.query('ALTER TABLE foobar ADD COLUMN name TEXT')
    await client.end()
  })

  it('can execute multiple statements in a transaction if ending cursor early', async () => {
    const client = new pg.Client()
    await client.connect()
    await client.query('begin')
    await client.query('CREATE TEMP TABLE foobar(id SERIAL PRIMARY KEY)')
    const cursor = client.query(new Cursor('SELECT * FROM foobar'))
    await new Promise((resolve) => cursor.close(resolve))
    await client.query('ALTER TABLE foobar ADD COLUMN name TEXT')
    await client.end()
  })

  it('can execute multiple statements in a transaction if no data', async () => {
    const client = new pg.Client()
    await client.connect()
    await client.query('begin')
    // create a cursor that has no data response
    const createText = 'CREATE TEMP TABLE foobar(id SERIAL PRIMARY KEY)'
    const cursor = client.query(new Cursor(createText))
    const err = await new Promise((resolve) => cursor.read(100, resolve))
    assert.ifError(err)
    await client.query('ALTER TABLE foobar ADD COLUMN name TEXT')
    await client.end()
  })
})
