CREATE TABLE IF NOT EXISTS attribution_customer_journey (
                                    conv_id text NOT NULL,
                                    session_id text NOT NULL,
                                    ihc real NOT NULL,
                                    PRIMARY KEY(conv_id,session_id)
                                );
