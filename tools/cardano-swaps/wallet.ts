/** Print the funding wallet's address and holdings. */
import { connect } from "./common";
const { wallet } = await connect();
console.log("address", wallet.address.toBech32());
console.log("payment key hash", wallet.address.getProps().paymentPart?.hash);
console.log("stake key hash  ", wallet.address.getProps().delegationPart?.hash);
const utxos = await wallet.getUnspentOutputs();
let ada = 0n; const assets = new Map<string, bigint>();
for (const u of utxos) {
  const v = u.output().amount(); ada += v.coin();
  for (const [k, n] of v.multiasset()?.entries() ?? []) assets.set(k, (assets.get(k) ?? 0n) + n);
}
console.log(`${utxos.length} utxos, ${Number(ada) / 1e6} ADA`);
for (const [k, n] of assets) console.log(" ", k, n.toString());
