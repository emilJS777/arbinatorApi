import app as app_module


def test_startup_migrations_enabled_by_default(monkeypatch):
    calls = []

    def fake_upgrade(directory, revision):
        calls.append((directory, revision))

    monkeypatch.delenv("AUTO_RUN_MIGRATIONS", raising=False)
    monkeypatch.delenv("MIGRATIONS_REVISION", raising=False)
    monkeypatch.setattr(app_module, "upgrade", fake_upgrade)

    assert app_module.run_startup_migrations() is True
    assert calls == [("migrations", "head")]


def test_startup_migrations_can_be_disabled(monkeypatch):
    calls = []
    monkeypatch.setenv("AUTO_RUN_MIGRATIONS", "false")
    monkeypatch.setattr(app_module, "upgrade", lambda **kwargs: calls.append(kwargs))

    assert app_module.run_startup_migrations() is False
    assert calls == []


def test_startup_migrations_uses_configured_revision(monkeypatch):
    calls = []

    def fake_upgrade(directory, revision):
        calls.append((directory, revision))

    monkeypatch.setenv("AUTO_RUN_MIGRATIONS", "true")
    monkeypatch.setenv("MIGRATIONS_REVISION", "af3b2c1d4e90")
    monkeypatch.setattr(app_module, "upgrade", fake_upgrade)

    assert app_module.run_startup_migrations() is True
    assert calls == [("migrations", "af3b2c1d4e90")]
