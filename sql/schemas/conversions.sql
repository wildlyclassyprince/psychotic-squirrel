CREATE TABLE IF NOT EXISTS conversions (
                                    conv_id text NOT NULL,
                                    user_id text NOT NULL,
                                    conv_date text NOT NULL,
                                    conv_time text NOT NULL,
                                    revenue real NOT NULL,
                                    PRIMARY KEY(conv_id)
                                );
