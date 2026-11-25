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
import concat from 'concat-stream'
import QueryStream from '../src'
import helper from './helper'

if (process.version.startsWith('v8.')) {
  console.error('warning! node less than 10lts stream closing semantics may not behave properly')
} else {
  helper('close', function (client) {
    it('emits close', function (done) {
      const stream = new QueryStream('SELECT * FROM generate_series(0, $1) num', [3], {
        batchSize: 2,
        highWaterMark: 2,
      })
      const query = client.query(stream)
      query.pipe(concat(function () {}))
      query.on('close', done)
    })
  })

  helper('early close', function (client) {
    it('can be closed early', function (done) {
      const stream = new QueryStream('SELECT * FROM generate_series(0, $1) num', [20000], {
        batchSize: 2,
        highWaterMark: 2,
      })
      const query = client.query(stream)
      let readCount = 0
      query.on('readable', function () {
        readCount++
        query.read()
      })
      query.once('readable', function () {
        query.destroy()
      })
      query.on('close', function () {
        assert(readCount < 10, 'should not have read more than 10 rows')
        done()
      })
    })

    it('can destroy stream while reading', function (done) {
      const stream = new QueryStream('SELECT * FROM generate_series(0, 100), pg_sleep(1)')
      client.query(stream)
      stream.on('data', () => done(new Error('stream should not have returned rows')))
      setTimeout(() => {
        stream.destroy()
        stream.on('close', done)
      }, 100)
    })

    it('emits an error when calling destroy with an error', function (done) {
      const stream = new QueryStream('SELECT * FROM generate_series(0, 100), pg_sleep(1)')
      client.query(stream)
      stream.on('data', () => done(new Error('stream should not have returned rows')))
      setTimeout(() => {
        stream.destroy(new Error('intentional error'))
        stream.on('error', (err) => {
          // make sure there's an error
          assert(err)
          assert.strictEqual(err.message, 'intentional error')
          done()
        })
      }, 100)
    })

    it('can destroy stream while reading an error', function (done) {
      const stream = new QueryStream('SELECT * from  pg_sleep(1), basdfasdf;')
      client.query(stream)
      stream.on('data', () => done(new Error('stream should not have returned rows')))
      stream.once('error', () => {
        stream.destroy()
        // wait a bit to let any other errors shake through
        setTimeout(done, 100)
      })
    })

    it('does not crash when destroying the stream immediately after calling read', function (done) {
      const stream = new QueryStream('SELECT * from generate_series(0, 100), pg_sleep(1);')
      client.query(stream)
      stream.on('data', () => done(new Error('stream should not have returned rows')))
      stream.destroy()
      stream.on('close', done)
    })

    it('does not crash when destroying the stream before its submitted', function (done) {
      const stream = new QueryStream('SELECT * from generate_series(0, 100), pg_sleep(1);')
      stream.on('data', () => done(new Error('stream should not have returned rows')))
      stream.destroy()
      stream.on('close', done)
    })
  })
}
