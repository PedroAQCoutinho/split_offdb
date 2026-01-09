DROP TABLE IF EXISTS split_output.cdt_v2025_unnest;

CREATE TABLE split_output.cdt_v2025_unnest AS
SELECT
    s.gid,
    unnest(s.id_layer) id_layer,
    unnest(s.id_feature) id_feature,
    s.hexadecimal,
    s.n_car,
    s.area_ha,
    s.cd_mun,
    s.cd_uf,
    s.cd_bioma,
    s.is_am,
    s.is_assa,
    s.is_assb,
    s.is_au,
    s.is_bioma,
    s.is_car,
    s.is_fpnd,
    s.is_gp,
    s.is_irp,
    s.is_md,
    s.is_mun,
    s.is_tih,
    s.is_tinh,
    s.is_tqd,
    s.is_tqnd,
    s.is_ucpi,
    s.is_ucus,
    s.is_ucusb,
    s.categorias_fundiarias_v2025
FROM split_output.cdt_v2025_split s

CREATE INDEX cdt_v2025_unnest_gid_idx
  ON split_output.cdt_v2025_unnest (gid);

CREATE INDEX idx_split_output_cdt_v2025_unnest_area_ha
  ON split_output.cdt_v2025_unnest (area_ha);

CREATE INDEX idx_split_output_cdt_v2025_unnest_id_feature
  ON split_output.cdt_v2025_unnest (id_feature);

CREATE INDEX idx_split_output_cdt_v2025_unnest_id_layer
  ON split_output.cdt_v2025_unnest (id_layer);

CREATE INDEX idx_split_output_cdt_v2025_unnest_is_am
  ON split_output.cdt_v2025_unnest (is_am);

CREATE INDEX idx_split_output_cdt_v2025_unnest_is_assa
  ON split_output.cdt_v2025_unnest (is_assa);

CREATE INDEX idx_split_output_cdt_v2025_unnest_is_assb
  ON split_output.cdt_v2025_unnest (is_assb);

CREATE INDEX idx_split_output_cdt_v2025_unnest_is_au
  ON split_output.cdt_v2025_unnest (is_au);

CREATE INDEX idx_split_output_cdt_v2025_unnest_is_bioma
  ON split_output.cdt_v2025_unnest (is_bioma);

CREATE INDEX idx_split_output_cdt_v2025_unnest_is_car
  ON split_output.cdt_v2025_unnest (is_car);

CREATE INDEX idx_split_output_cdt_v2025_unnest_is_fpnd
  ON split_output.cdt_v2025_unnest (is_fpnd);

CREATE INDEX idx_split_output_cdt_v2025_unnest_n_car
  ON split_output.cdt_v2025_unnest (n_car);
