from dvc.repo.experiments.serialize import SerializableExp


def test_serialize_external_paths_include_protocol(dvc, mocker):
    """External paths should include protocol prefix (e.g. s3://)."""
    mock_dep = mocker.MagicMock(fs_path="bucket/data.csv", is_in_repo=False)
    mock_dep.fs.unstrip_protocol.return_value = "s3://bucket/data.csv"

    mock_out = mocker.MagicMock(
        fs_path="bucket/model.pkl", is_in_repo=False, is_metric=False, is_plot=False
    )
    mock_out.fs.unstrip_protocol.return_value = "s3://bucket/model.pkl"

    mocker.patch.object(
        type(dvc.index), "deps", mocker.PropertyMock(return_value=[mock_dep])
    )
    mocker.patch.object(
        type(dvc.index), "outs", mocker.PropertyMock(return_value=[mock_out])
    )
    mocker.patch.object(dvc, "get_rev", return_value="abc123")

    result = SerializableExp.from_repo(dvc)

    assert "s3://bucket/data.csv" in result.deps
    assert "s3://bucket/model.pkl" in result.outs
