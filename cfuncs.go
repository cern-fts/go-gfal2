/*
 * Copyright (c) CERN 2016
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gfal2

/*
#include <gfal_api.h>

void logHandlerWrapper(const char*, GLogLevelFlags, const char*, gpointer);
void eventCallbackWrapper(const gfalt_event_t e, gpointer user_data);
void monitorCallbackWrapper(gfalt_transfer_status_t h, const char* src, const char *dst, gpointer user_data);

void logCallback(const gchar *log_domain, GLogLevelFlags log_level,
	const gchar *message, gpointer user_data)
{
	logHandlerWrapper(log_domain, log_level, message, user_data);
}


void monitorCallback(gfalt_transfer_status_t h, const char* src, const char* dst, gpointer user_data)
{
	monitorCallbackWrapper(h, src, dst, user_data);
}


void eventCallback(const gfalt_event_t e, gpointer user_data)
{
	eventCallbackWrapper(e, user_data);
}

*/
import "C"
