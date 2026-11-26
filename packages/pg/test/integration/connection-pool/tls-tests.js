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

const fs = require('fs')

const helper = require('./test-helper')
const pg = helper.pg

const suite = new helper.Suite()

if (process.env.PG_CLIENT_CERT_TEST) {
  suite.testAsync('client certificate', async () => {
    const pool = new pg.Pool({
      ssl: {
        ca: fs.readFileSync(process.env.PGSSLROOTCERT),
        cert: fs.readFileSync(process.env.PGSSLCERT),
        key: fs.readFileSync(process.env.PGSSLKEY),
      },
    })

    await pool.query('SELECT 1')
    await pool.end()
  })
}
