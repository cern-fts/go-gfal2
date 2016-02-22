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
