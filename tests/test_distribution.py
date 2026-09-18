import pathlib
import tomllib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]


class DistributionMetadataTests(unittest.TestCase):
    def test_bridge_metadata_points_to_owned_fork(self):
        data = tomllib.loads((ROOT / "pyproject.toml").read_text())
        self.assertEqual(data["project"]["version"], "0.9.1+awg31.m2")
        self.assertEqual(data["project"]["url"], "https://github.com/GamerKhaan/node_bridge_py")
        self.assertEqual(data["project"]["urls"]["Repository"], "https://github.com/GamerKhaan/node_bridge_py.git")


if __name__ == "__main__":
    unittest.main()
