/**
 * Shared pieces for the Cardano-Swaps testnet tooling: provider, wallet, the
 * mainnet script bytes, beacon derivation, and the one-way `SwapDatum`.
 *
 * Env: BLOCKFROST_PROJECT_ID (preview), SEED_PHRASE (the funding wallet).
 * `run.sh` sets both.
 */
import {
  Address, AddressType, AssetName, Bip32PrivateKey, ConstrPlutusData, CredentialType,
  Datum, HexBlob, NetworkId, PlutusData, PlutusList, PlutusV3Script, PolicyId,
  Script, TransactionId, TransactionInput, mnemonicToEntropy, wordlist,
  type Hash28ByteBase16,
} from "@blaze-cardano/core";
import { Blaze } from "@blaze-cardano/sdk";
import { Blockfrost } from "@blaze-cardano/query";
import { HotWallet } from "@blaze-cardano/wallet";
import { sha256 } from "@noble/hashes/sha256";
import { readFileSync } from "fs";

export const NETWORK = (process.env.NETWORK ?? "preview") as "preview" | "preprod";

export async function connect() {
  const projectId = process.env.BLOCKFROST_PROJECT_ID;
  const seed = process.env.SEED_PHRASE;
  if (!projectId || !seed) throw new Error("BLOCKFROST_PROJECT_ID and SEED_PHRASE are required (use run.sh)");
  const provider = new Blockfrost({ network: `cardano-${NETWORK}`, projectId });
  const entropy = mnemonicToEntropy(seed, wordlist);
  const master = Bip32PrivateKey.fromBip39Entropy(Buffer.from(entropy), "");
  const wallet = await HotWallet.fromMasterkey(master.hex(), provider);
  const blaze = await Blaze.from(provider, wallet);
  return { provider, wallet, blaze };
}

// ─── Scripts ───────────────────────────────────────────────────────────────

const here = new URL(".", import.meta.url).pathname;
function loadScript(file: string, expectHash: string): Script {
  const hex = readFileSync(`${here}scripts/${file}`, "utf8").trim();
  const script = Script.newPlutusV3Script(new PlutusV3Script(HexBlob(hex)));
  const hash = script.hash().toString();
  if (hash !== expectHash) throw new Error(`${file}: hash ${hash}, expected ${expectHash}`);
  return script;
}

export const SWAP_HASH = "e5a22e4c31db20bce1c8b081f8e4009683990a33157947d75030deb8";
export const BEACON_HASH = "274765b4c626c28d18752176b59c0ff63db56b8305c1daa49c9879fe";
export const swapScript = () => loadScript("one_way_swap.v3.cbor.hex", SWAP_HASH);
export const beaconScript = () => loadScript("one_way_beacon.v3.cbor.hex", BEACON_HASH);

// ─── Beacons: sha2_256(serialise_data(ctor)) with indefinite-array fields ──

export type Asset = { policy: string; name: string }; // hex; ADA = {"",""}
export const ADA: Asset = { policy: "", name: "" };

function cborBytes(hex: string): Uint8Array {
  const b = Buffer.from(hex, "hex");
  if (b.length < 24) return Buffer.concat([Buffer.from([0x40 + b.length]), b]);
  if (b.length < 256) return Buffer.concat([Buffer.from([0x58, b.length]), b]);
  throw new Error("bytestring too long");
}
function cborConstr(idx: number, fields: string[]): Uint8Array {
  return Buffer.concat([Buffer.from([0xd8, 121 + idx, 0x9f]), ...fields.map(cborBytes), Buffer.from([0xff])]);
}
const hex = (b: Uint8Array) => Buffer.from(b).toString("hex");
export const offerBeacon = (a: Asset) => hex(sha256(cborConstr(0, [a.policy, a.name])));
export const askBeacon = (a: Asset) => hex(sha256(cborConstr(1, [a.policy, a.name])));
export const pairBeacon = (offer: Asset, ask: Asset) =>
  hex(sha256(cborConstr(2, [offer.policy, offer.name, ask.policy, ask.name])));

// The repo's golden vector and a live mainnet beacon; fails loudly if the
// derivation drifts.
if (offerBeacon(ADA) !== "07d5f63e85046b83e1fc4102a7c19c3f1711c51984725e3b6cf195900947cebe")
  throw new Error("offer beacon derivation is wrong");
if (pairBeacon(ADA, { policy: "c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad", name: "0014df105553444d" })
    !== "c35c0f3115aec28da91f69abd64195d2731a5e600af8e61ca5aab940916652b8")
  throw new Error("pair beacon derivation is wrong");

// ─── SwapDatum ─────────────────────────────────────────────────────────────

const bytes = (h: string) => PlutusData.newBytes(Buffer.from(h, "hex"));
const int = (n: bigint) => PlutusData.newInteger(n);
export function constr(tag: number, fields: PlutusData[]): PlutusData {
  const l = new PlutusList();
  for (const f of fields) l.add(f);
  return PlutusData.newConstrPlutusData(new ConstrPlutusData(BigInt(tag), l));
}
export const none = () => constr(1, []);

/** One-way `SwapDatum` for a fresh order (no prev_input, no expiration). */
export function swapDatum(offer: Asset, ask: Asset, priceNum: bigint, priceDen: bigint): PlutusData {
  return constr(0, [
    bytes(BEACON_HASH),
    bytes(pairBeacon(offer, ask)),
    bytes(offer.policy), bytes(offer.name), bytes(offerBeacon(offer)),
    bytes(ask.policy), bytes(ask.name), bytes(askBeacon(ask)),
    constr(0, [int(priceNum), int(priceDen)]),
    none(),
    none(),
  ]);
}

/** Swap-script address carrying the wallet's stake key (the validator requires one). */
export function swapAddress(wallet: HotWallet): Address {
  const stake = wallet.address.getProps().delegationPart;
  if (!stake) throw new Error("wallet has no stake credential; the beacon script requires one");
  return new Address({
    type: AddressType.BasePaymentScriptStakeKey,
    networkId: NetworkId.Testnet,
    paymentPart: { type: CredentialType.ScriptHash, hash: SWAP_HASH as Hash28ByteBase16 },
    delegationPart: stake,
  });
}

export const outRef = (s: string) => {
  const [tx, idx] = s.split("#");
  return new TransactionInput(TransactionId(tx), BigInt(idx));
};
export { Datum, PolicyId, AssetName, PlutusData };
