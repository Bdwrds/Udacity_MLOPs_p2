#!/usr/bin/env python
"""
Basic clean and upload to W&B
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    logger.info("Downloading data from W&B")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info("Load data and apply basic clean on price")
    df = pd.read_csv(artifact_local_path)
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    logger.info("Write cleaned df to csv")
    df.to_csv(args.output_artifact, index=False)

    logger.info("Upload clean data to W&B")
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(args.output_artifact)
    run.log_artifact(artifact)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This step cleans the data")


    parser.add_argument(
        "--input_artifact",
        type=str,
        help="Name of input_artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Name of output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="Output artifact type",
        required=True
    )
    parser.add_argument(
        "--output_description",
        type=str,
        help="Description of output",
        required=True
    )

    parser.add_argument(
        "--min_price",
        type=int,
        help="Min valid price",
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=int,
        help="Max valid price",
        required=True
    )

    args = parser.parse_args()

    go(args)
