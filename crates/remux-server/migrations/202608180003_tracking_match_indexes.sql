-- Tracking imports match provider IDs inside the external_ids JSON document.
-- Putting kind first keeps movie/series/episode namespaces separate and makes
-- each sequential equality lookup usable without scanning a media kind.
CREATE INDEX IF NOT EXISTS idx_media_kind_external_imdb
    ON media(kind, json_extract(external_ids, '$.imdb'));

CREATE INDEX IF NOT EXISTS idx_media_kind_external_tmdb
    ON media(kind, CAST(json_extract(external_ids, '$.tmdb') AS INTEGER));

CREATE INDEX IF NOT EXISTS idx_media_kind_external_tvdb
    ON media(kind, CAST(json_extract(external_ids, '$.tvdb') AS INTEGER));

-- Simkl episode rows identify their series, not the episode itself. Once the
-- series is resolved, this index finds a TVDB-shaped season/episode coordinate
-- directly. The season-parent fallback uses idx_media_parent_kind_idx.
CREATE INDEX IF NOT EXISTS idx_media_grandparent_kind_parent_idx_idx
    ON media(grandparent_id, kind, parent_idx, idx)
    WHERE grandparent_id IS NOT NULL;
