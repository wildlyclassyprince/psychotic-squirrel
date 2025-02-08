CREATE TABLE IF NOT EXISTS session_sources (
                                    session_id text NOT NULL,
                                    user_id text NOT NULL,
                                    event_date text NOT NULL,
                                    event_time text NOT NULL,
                                    channel_name text NOT NULL,
                                    holder_engagement INTEGER NOT NULL,
                                    closer_engagement INTEGER NOT NULL,
                                    impression_interaction INTEGER NOT NULL,
                                    PRIMARY KEY(session_id)
                                );
