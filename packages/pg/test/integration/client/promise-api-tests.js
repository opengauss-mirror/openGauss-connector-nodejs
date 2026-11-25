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

const helper = require('./test-helper')
const pg = helper.pg

const suite = new helper.Suite()

suite.test('valid connection completes promise', () => {
  const client = new pg.Client()
  return client.connect().then(() => {
    return client.end().then(() => {})
  })
})

suite.test('valid connection completes promise', () => {
  const client = new pg.Client()
  return client.connect().then(() => {
    return client.end().then(() => {})
  })
})

suite.test('invalid connection rejects promise', (done) => {
  const client = new pg.Client({ host: 'alksdjflaskdfj', port: 1234 })
  return client.connect().catch((e) => {
    assert(e instanceof Error)
    done()
  })
})

suite.test('connected client does not reject promise after connection', (done) => {
  const client = new pg.Client()
  return client.connect().then(() => {
    setTimeout(() => {
      client.on('error', (e) => {
        assert(e instanceof Error)
        client.end()
        done()
      })
      // manually kill the connection
      client.emit('error', new Error('something bad happened...but not really'))
    }, 50)
  })
})
