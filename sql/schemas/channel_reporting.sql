CREATE TABLE IF NOT EXISTS channel_reporting (
                            channel_name text NOT NULL,
                            date text NOT NULL,
                            cost real NOT NULL,
                            ihc real NOT NULL,
                            ihc_revenue real NOT NULL,
                            PRIMARY KEY(channel_name,date)
                        );
