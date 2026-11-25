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

const expect = require('expect.js')
const co = require('co')
const _ = require('lodash')

const describe = require('mocha').describe
const it = require('mocha').it

const Pool = require('../')

describe('pool size of 1', () => {
  it(
    'can create a single client and use it once',
    co.wrap(function* () {
      const pool = new Pool({ max: 1 })
      expect(pool.waitingCount).to.equal(0)
      const client = yield pool.connect()
      const res = yield client.query('SELECT $1::text as name', ['hi'])
      expect(res.rows[0].name).to.equal('hi')
      client.release()
      pool.end()
    })
  )

  it(
    'can create a single client and use it multiple times',
    co.wrap(function* () {
      const pool = new Pool({ max: 1 })
      expect(pool.waitingCount).to.equal(0)
      const client = yield pool.connect()
      const wait = pool.connect()
      expect(pool.waitingCount).to.equal(1)
      client.release()
      const client2 = yield wait
      expect(client).to.equal(client2)
      client2.release()
      return yield pool.end()
    })
  )

  it(
    'can only send 1 query at a time',
    co.wrap(function* () {
      const pool = new Pool({ max: 1 })

      // the query text column name changed in PostgreSQL 9.2
      const versionResult = yield pool.query('SHOW server_version_num')
      const version = parseInt(versionResult.rows[0].server_version_num, 10)
      const queryColumn = version < 90200 ? 'current_query' : 'query'

      const queryText = 'SELECT COUNT(*) as counts FROM pg_stat_activity WHERE ' + queryColumn + ' = $1'
      const queries = _.times(20, () => pool.query(queryText, [queryText]))
      const results = yield Promise.all(queries)
      const counts = results.map((res) => parseInt(res.rows[0].counts, 10))
      expect(counts).to.eql(_.times(20, (i) => 1))
      return yield pool.end()
    })
  )
})
