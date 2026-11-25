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
var args = require('../test/cli')
var pg = require('../lib')

var people = [
  { name: 'Aaron', age: 10 },
  { name: 'Brian', age: 20 },
  { name: 'Chris', age: 30 },
  { name: 'David', age: 40 },
  { name: 'Elvis', age: 50 },
  { name: 'Frank', age: 60 },
  { name: 'Grace', age: 70 },
  { name: 'Haley', age: 80 },
  { name: 'Irma', age: 90 },
  { name: 'Jenny', age: 100 },
  { name: 'Kevin', age: 110 },
  { name: 'Larry', age: 120 },
  { name: 'Michelle', age: 130 },
  { name: 'Nancy', age: 140 },
  { name: 'Olivia', age: 150 },
  { name: 'Peter', age: 160 },
  { name: 'Quinn', age: 170 },
  { name: 'Ronda', age: 180 },
  { name: 'Shelley', age: 190 },
  { name: 'Tobias', age: 200 },
  { name: 'Uma', age: 210 },
  { name: 'Veena', age: 220 },
  { name: 'Wanda', age: 230 },
  { name: 'Xavier', age: 240 },
  { name: 'Yoyo', age: 250 },
  { name: 'Zanzabar', age: 260 },
]

var con = new pg.Client({
  host: args.host,
  port: args.port,
  user: args.user,
  password: args.password,
  database: args.database,
})

con.connect((err) => {
  if (err) {
    throw err
  }

  con.query(
    'DROP TABLE IF EXISTS person;' + ' CREATE TABLE person (id serial, name varchar(10), age integer)',
    (err) => {
      if (err) {
        throw err
      }

      console.log('Created table person')
      console.log('Filling it with people')

      con.query(
        'INSERT INTO person (name, age) VALUES' +
          people.map((person) => ` ('${person.name}', ${person.age})`).join(','),
        (err, result) => {
          if (err) {
            throw err
          }

          console.log(`Inserted ${result.rowCount} people`)
          con.end()
        }
      )
    }
  )
})
