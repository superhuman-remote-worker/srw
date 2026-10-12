"""Pre-SSH stop has only the namespaced mutations its exact actuator uses."""

from tests.test_manifest_hosting_helm import render


def test_controller_stop_and_finalizer_permissions_are_namespaced_and_narrow():
    docs = render(
        "vm.mode=same-cluster",
        "agent.tailscale.enabled=false",
        "vm.lifecycleAuthSecretName=pre-ssh-auth",
        "vmController.persistentRootdisk.enabled=true",
    )
    role = next(
        doc
        for doc in docs
        if doc["kind"] == "Role" and doc["metadata"]["name"].endswith("-vm-controller")
    )
    rules = role["rules"]
    assert {
        (tuple(rule["apiGroups"]), tuple(rule["resources"]), tuple(rule["verbs"]))
        for rule in rules
        if rule["resources"] in (["pods/finalizers"], ["virtualmachines"])
    } == {
        (("",), ("pods/finalizers",), ("patch",)),
        (("kubevirt.io",), ("virtualmachines",), ("patch",)),
    }
    node = next(
        doc
        for doc in docs
        if doc["kind"] == "ClusterRole"
        and doc["metadata"]["name"].endswith("-vm-controller-node-observer")
    )
    assert node["rules"] == [
        {"apiGroups": [""], "resources": ["nodes"], "verbs": ["get"]}
    ]
