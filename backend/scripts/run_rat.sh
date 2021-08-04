source /houston2/pritam/activate_conda.sh

conda activate /houston2/pritam/rat_mekong_v3/.condaenv

python /houston2/pritam/rat_mekong_v3/backend/scripts/utils/update_meta.py

python /houston2/pritam/rat_mekong_v3/backend/scripts/run_rat.py