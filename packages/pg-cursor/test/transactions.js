/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/mit
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
