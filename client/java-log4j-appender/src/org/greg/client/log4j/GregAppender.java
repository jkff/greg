package org.greg.client.log4j;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;
import org.greg.client.Greg;

public class GregAppender extends AppenderSkeleton {
    @Override
    protected void append(LoggingEvent loggingEvent) {
        Layout layout = getLayout();
        Greg.log(layout == null ? loggingEvent.getRenderedMessage() : layout.format(loggingEvent));
    }

    public void close() {
        // Nothing
    }

    public boolean requiresLayout() {
        return false;
    }
}
