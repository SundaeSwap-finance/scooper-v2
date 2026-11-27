/** Resolve outrefs on the configured network and print what they hold. */
import { connect, outRef } from "./common";
const { provider } = await connect();
const utxos = await provider.resolveUnspentOutputs(process.argv.slice(2).map(outRef));
for (const u of utxos) {
  const o = u.output();
  console.log(`${u.input().transactionId()}#${u.input().index()}`);
  console.log("  address", o.address().toBech32());
  console.log("  lovelace", o.amount().coin().toString(), "assets", o.amount().multiasset()?.size ?? 0);
  console.log("  script ref", o.scriptRef()?.hash().toString() ?? "none");
  console.log("  datum", o.datum()?.asInlineData()?.toCbor().slice(0, 120) ?? "none");
}
if (!utxos.length) console.log("not found (unspent) — not yet on chain, or spent");
