-- Anime catalogs historically stored MAL/AniList/Kitsu identifiers only in
-- `custom_stremio_id`. Populate the typed fields used by tracking integrations
-- without overwriting IDs that a metadata provider has already supplied.

UPDATE media
SET external_ids = json_set(
    external_ids,
    '$.mal',
    CAST(substr(json_extract(external_ids, '$.custom_stremio_id'), 5) AS INTEGER)
)
WHERE json_extract(external_ids, '$.mal') IS NULL
  AND json_extract(external_ids, '$.custom_stremio_id') LIKE 'mal:%'
  AND length(substr(json_extract(external_ids, '$.custom_stremio_id'), 5)) > 0
  AND substr(json_extract(external_ids, '$.custom_stremio_id'), 5) NOT GLOB '*[^0-9]*'
  AND CAST(substr(json_extract(external_ids, '$.custom_stremio_id'), 5) AS INTEGER) > 0;

UPDATE media
SET external_ids = json_set(
    external_ids,
    '$.anilist',
    CAST(substr(json_extract(external_ids, '$.custom_stremio_id'), 9) AS INTEGER)
)
WHERE json_extract(external_ids, '$.anilist') IS NULL
  AND json_extract(external_ids, '$.custom_stremio_id') LIKE 'anilist:%'
  AND length(substr(json_extract(external_ids, '$.custom_stremio_id'), 9)) > 0
  AND substr(json_extract(external_ids, '$.custom_stremio_id'), 9) NOT GLOB '*[^0-9]*'
  AND CAST(substr(json_extract(external_ids, '$.custom_stremio_id'), 9) AS INTEGER) > 0;

UPDATE media
SET external_ids = json_set(
    external_ids,
    '$.kitsu',
    CAST(substr(json_extract(external_ids, '$.custom_stremio_id'), 7) AS INTEGER)
)
WHERE json_extract(external_ids, '$.kitsu') IS NULL
  AND json_extract(external_ids, '$.custom_stremio_id') LIKE 'kitsu:%'
  AND length(substr(json_extract(external_ids, '$.custom_stremio_id'), 7)) > 0
  AND substr(json_extract(external_ids, '$.custom_stremio_id'), 7) NOT GLOB '*[^0-9]*'
  AND CAST(substr(json_extract(external_ids, '$.custom_stremio_id'), 7) AS INTEGER) > 0;
