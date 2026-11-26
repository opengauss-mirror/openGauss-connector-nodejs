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

export function parse(connectionString: string): ConnectionOptions

export interface ConnectionOptions {
  host: string | null
  password?: string
  user?: string
  port?: string | null
  database: string | null | undefined
  client_encoding?: string
  ssl?: boolean | string

  application_name?: string
  fallback_application_name?: string
  options?: string
}
