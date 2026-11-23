# Checking that a new run environment gives the same results
When something about where or how the framework runs changes (a new EC2 setup or launch template, AWS account,
Docker base image or dependency), check that it still reproduces the published outputs before relying on it:

1. Pick a small part of the framework that has a published output, such as the tree cover loss from fires (TCLF)
   input for one tile (a 16 MB tile).
2. If you can, make and compare it in the current environment first, to confirm the published output can be reproduced at all.
3. Make and compare it in the new environment.

This shows that the same code and inputs give the same outputs in the new environment. It doesn't show that the outputs are correct.

In the Docker container (see [Building the Docker container](../readme.md#building-the-docker-container), or
[Running on AWS with just](../readme.md#running-on-aws-with-just) for an instance in land-research):
```
python -m data_prep.mp_prep_other_inputs_annual -l 00N_110E -p tclf -nu
python -m analyses.compare_tiles /usr/local/tiles/00N_110E_tree_cover_loss_fire_processed.tif s3://gfw2-data/climate/carbon_model/other_emissions_inputs/tree_cover_loss_fires/20260317/processed/00N_110E_tree_cover_loss_fire_processed.tif
```
`analyses/compare_tiles.py` compares pixel values, not file bytes, because GeoTIFFs record when they were made.
Before comparing, it checks that the comparison can fail: it changes one pixel in a copy of the reference tile and
confirms that it finds exactly one difference. It exits with 0 if the tiles' pixels are identical, 1 if they differ,
and 2 if that check fails. If the new tile isn't at the path above, `ls /usr/local/tiles/*fire_processed*` shows where it went.

Notes:
- `-nu` keeps the output and the log off s3. Don't use `-d` with uploads on for this: it replaces the last 8 characters
  of the output folder, so for TCLF it uploads into the published 2025 folder (`.../tree_cover_loss_fires/20260317/p<date>/`).
- `20260317` is the run date of the published 2025 TCLF tiles. For other parts of the framework, the published
  folders are in `constants_and_names.py`.
- With temporary AWS credentials (e.g. from SSO), the container needs `AWS_SESSION_TOKEN` as well as the key and secret.
  Without it, downloads fail with a misleading "Tile_id not valid" error.
