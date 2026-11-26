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
