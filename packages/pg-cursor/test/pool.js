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
const assert = require('assert')
const Cursor = require('../')
const pg = require('pg')

const text = 'SELECT generate_series as num FROM generate_series(0, 50)'

function poolQueryPromise(pool, readRowCount) {
  return new Promise((resolve, reject) => {
    pool.connect((err, client, done) => {
      if (err) {
        done(err)
        return reject(err)
      }
      const cursor = client.query(new Cursor(text))
      cursor.read(readRowCount, (err) => {
        if (err) {
          done(err)
          return reject(err)
        }
        cursor.close((err) => {
          if (err) {
            done(err)
            return reject(err)
          }
          done()
          resolve()
        })
      })
    })
  })
}

describe('pool', function () {
  beforeEach(function () {
    this.pool = new pg.Pool({ max: 1 })
  })

  afterEach(function () {
    this.pool.end()
  })

  it('closes cursor early, single pool query', function (done) {
    poolQueryPromise(this.pool, 25)
      .then(() => done())
      .catch((err) => {
        assert.ifError(err)
        done()
      })
  })

  it('closes cursor early, saturated pool', function (done) {
    const promises = []
    for (let i = 0; i < 10; i++) {
      promises.push(poolQueryPromise(this.pool, 25))
    }
    Promise.all(promises)
      .then(() => done())
      .catch((err) => {
        assert.ifError(err)
        done()
      })
  })

  it('closes exhausted cursor, single pool query', function (done) {
    poolQueryPromise(this.pool, 100)
      .then(() => done())
      .catch((err) => {
        assert.ifError(err)
        done()
      })
  })

  it('closes exhausted cursor, saturated pool', function (done) {
    const promises = []
    for (let i = 0; i < 10; i++) {
      promises.push(poolQueryPromise(this.pool, 100))
    }
    Promise.all(promises)
      .then(() => done())
      .catch((err) => {
        assert.ifError(err)
        done()
      })
  })

  it('can close multiple times on a pool', async function () {
    const pool = new pg.Pool({ max: 1 })
    const run = async () => {
      const cursor = new Cursor(text)
      const client = await pool.connect()
      client.query(cursor)
      await new Promise((resolve) => {
        cursor.read(25, function (err) {
          assert.ifError(err)
          cursor.close(function (err) {
            assert.ifError(err)
            client.release()
            resolve()
          })
        })
      })
    }
    await Promise.all([run(), run(), run()])
    await pool.end()
  })
})
