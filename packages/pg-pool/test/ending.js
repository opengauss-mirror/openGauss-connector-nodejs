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

'use strict'
const co = require('co')
const expect = require('expect.js')

const describe = require('mocha').describe
const it = require('mocha').it

const Pool = require('../')

describe('pool ending', () => {
  it('ends without being used', (done) => {
    const pool = new Pool()
    pool.end(done)
  })

  it('ends with a promise', () => {
    return new Pool().end()
  })

  it(
    'ends with clients',
    co.wrap(function* () {
      const pool = new Pool()
      const res = yield pool.query('SELECT $1::text as name', ['brianc'])
      expect(res.rows[0].name).to.equal('brianc')
      return pool.end()
    })
  )

  it(
    'allows client to finish',
    co.wrap(function* () {
      const pool = new Pool()
      const query = pool.query('SELECT $1::text as name', ['brianc'])
      yield pool.end()
      const res = yield query
      expect(res.rows[0].name).to.equal('brianc')
    })
  )
})
