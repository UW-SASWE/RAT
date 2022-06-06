# Run model - 
d=$(date +'%Y-%m-%d')
echo "Analysis Date: $(date)"

cd /houston2/pritam/rat_mekong_v3

source /houston2/pritam/activate_conda.sh
conda activate /houston2/pritam/rat_mekong_v3/.condaenv
echo "Activated Environment: "$CONDA_DEFAULT_ENV

# First checkout the version of code to use
git checkout v0.3.3
git status

python /houston2/pritam/rat_mekong_v3/backend/scripts/utils/update_meta.py

python /houston2/pritam/rat_mekong_v3/backend/scripts/run_rat.py

# PUBLISH
echo "Publishing Results"
conda activate /houston2/pritam/rat_mekong_v3/.paramiko
echo "Activated Environment: "$CONDA_DEFAULT_ENV
python /houston2/pritam/rat_mekong_v3/backend/scripts/_send_files.py