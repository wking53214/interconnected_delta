"""The README's recipe for the freshness limit, executed.

A signed checkpoint carries no freshness, so an older signed package, with the ledger cut back to match, verifies against its own
checkpoint. The recipe closes that from outside: the deployer keeps the newest checkpoint in storage the producer cannot rewrite and
hands THAT to the verifier, not the checkpoint that arrived with the package.
"""
import pytest
from store_support import cp_from, ledger_of

from delta import verify_export


def test_an_older_package_verifies_against_its_own_checkpoint_but_not_against_the_newest_one_held_outside():
    grown = ledger_of(5)
    older_export = ledger_of(3).export()  # the ledger as it was at length 3
    arrives_with_package = cp_from(grown, 3)
    newest_held_outside = cp_from(grown, 5)
    verify_export(older_export, checkpoint=arrives_with_package)  # the gap: internally consistent, so it verifies
    with pytest.raises(ValueError):
        verify_export(older_export, checkpoint=newest_held_outside)  # the recipe: the newest checkpoint names history it lacks


def test_the_current_package_still_verifies_against_the_newest_checkpoint():
    grown = ledger_of(5)
    verify_export(grown.export(), checkpoint=cp_from(grown, 5))


def test_a_package_that_is_ahead_of_the_newest_checkpoint_verifies_and_shows_the_unwitnessed_tail():
    grown = ledger_of(5)
    verify_export(grown.export(), checkpoint=cp_from(grown, 3))  # entries after the held checkpoint are not vouched for by it
