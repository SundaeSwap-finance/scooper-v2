/**
 * Create a one-way order: offer lovelace, ask a token, at a limit price
 * (ask units per offer unit, as a fraction).
 *
 *   run.sh create-order.ts <beacon-ref-utxo> <offer-lovelace> <ask-policy> <ask-name-hex> <num>/<den>
 */
import { makeValue } from "@blaze-cardano/tx";
import { makeUplcEvaluator } from "@blaze-cardano/vm";
import { SLOT_CONFIG_NETWORK, type Ed25519KeyHashHex } from "@blaze-cardano/core";
import {
  connect, outRef, swapDatum, swapAddress, pairBeacon, offerBeacon, askBeacon,
  ADA, BEACON_HASH, Datum, PolicyId, AssetName, constr,
} from "./common";

const [beaconRef, offerStr, askPolicy, askName, price] = process.argv.slice(2);
if (!price) throw new Error("usage: create-order.ts <beacon-ref-utxo> <offer-lovelace> <ask-policy> <ask-name-hex> <num>/<den>");
const offer = BigInt(offerStr);
const [num, den] = price.split("/").map(BigInt);
const ask = { policy: askPolicy, name: askName };

const { blaze, provider, wallet } = await connect();
const [beaconUtxo] = await provider.resolveUnspentOutputs([outRef(beaconRef)]);
if (!beaconUtxo) throw new Error(`beacon ref UTxO ${beaconRef} not found`);

const beacons = [pairBeacon(ADA, ask), offerBeacon(ADA), askBeacon(ask)];
const mint = new Map(beacons.map((b) => [AssetName(b), 1n] as const));
const value = makeValue(offer, ...beacons.map((b) => [BEACON_HASH + b, 1n] as [string, bigint]));
const addr = swapAddress(wallet);
const stakeKeyHash = wallet.address.getProps().delegationPart!.hash as Ed25519KeyHashHex;

// Local evaluation surfaces the validator's trace on failure; Blockfrost does not.
const params = await provider.getParameters();
const evaluator = makeUplcEvaluator(params, 1.1, 1.1, SLOT_CONFIG_NETWORK.Preview);
const tx = await blaze
  .newTransaction()
  .useEvaluator(evaluator)
  .addReferenceInput(beaconUtxo)
  .addMint(PolicyId(BEACON_HASH), mint, constr(0, []))
  .lockAssets(addr, value, Datum.newInlineData(swapDatum(ADA, ask, num, den)))
  // The beacon script requires the order's staking credential to approve.
  .addRequiredSigner(stakeKeyHash)
  .complete();
// Blaze's default signing uses the payment key only; the stake key must sign too.
const witnesses = await wallet.signTransaction(tx, true, true);
const ws = tx.witnessSet();
ws.setVkeys(witnesses.vkeys()!);
tx.setWitnessSet(ws);
const signed = tx;
const id = await blaze.provider.postTransactionToChain(signed);
console.log("order address", addr.toBech32());
console.log("tx", id, "— order UTxO is the output at", addr.toBech32().slice(0, 20) + "…");
