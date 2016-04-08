    create table scrobbles (id serial primary key, lastfm_id varchar(2048) not null, spotify_id varchar(2048), has_spotify_id boolean default false);
    create index on scrobbles (lastfm_id);
    create index on scrobbles (spotify_id);
    create index on scrobbles (has_spotify_id);
