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

'use strict'
const assert = require('assert')
const Cursor = require('../')
const pg = require('pg')

describe('query config passed to result', () => {
  it('passes rowMode to result', (done) => {
    const client = new pg.Client()
    client.connect()
    const text = 'SELECT generate_series as num FROM generate_series(0, 5)'
    const cursor = client.query(new Cursor(text, null, { rowMode: 'array' }))
    cursor.read(10, (err, rows) => {
      assert(!err)
      assert.deepStrictEqual(rows, [[0], [1], [2], [3], [4], [5]])
      client.end()
      done()
    })
  })

  it('passes types to result', (done) => {
    const client = new pg.Client()
    client.connect()
    const text = 'SELECT generate_series as num FROM generate_series(0, 2)'
    const types = {
      getTypeParser: () => () => 'foo',
    }
    const cursor = client.query(new Cursor(text, null, { types }))
    cursor.read(10, (err, rows) => {
      assert(!err)
      assert.deepStrictEqual(rows, [{ num: 'foo' }, { num: 'foo' }, { num: 'foo' }])
      client.end()
      done()
    })
  })
})
