/**
 * Publish the one-way swap and beacon scripts as reference-script UTxOs at
 * the funding wallet's own address.
 *
 *   publish-ref.ts prepare <file>   build + sign, write the tx to <file>, print its id
 *   publish-ref.ts submit  <file>   submit a prepared tx
 *
 * Two steps because the scooper indexes a ref UTxO only when it processes
 * the block that creates it: configure the scooper with the id first, restart
 * it, then submit. `${id}#0` is the swap ref (the scooper's `swap-ref-utxo`),
 * `${id}#1` the beacon ref (create-order.ts).
 */
import { Transaction, TransactionOutput, Value, HexBlob } from "@blaze-cardano/core";
import { readFileSync, writeFileSync } from "fs";
import { connect, swapScript, beaconScript } from "./common";

const [mode, file] = process.argv.slice(2);
if (!file || !["prepare", "submit"].includes(mode)) throw new Error("usage: publish-ref.ts prepare|submit <file>");
const { wallet, blaze } = await connect();

if (mode === "prepare") {
  const refOutput = (script: ReturnType<typeof swapScript>) => {
    const out = new TransactionOutput(wallet.address, new Value(20_000_000n));
    out.setScriptRef(script);
    return out;
  };
  const tx = await blaze
    .newTransaction()
    .addOutput(refOutput(swapScript()))
    .addOutput(refOutput(beaconScript()))
    .complete();
  const signed = await blaze.signTransaction(tx);
  writeFileSync(file, signed.toCbor());
  const id = signed.getId();
  console.log("prepared", file);
  console.log("swap-ref-utxo  ", `${id}#0`);
  console.log("beacon-ref-utxo", `${id}#1`);
} else {
  const signed = Transaction.fromCbor(HexBlob(readFileSync(file, "utf8").trim()));
  const id = await blaze.provider.postTransactionToChain(signed);
  console.log("submitted", id);
}
