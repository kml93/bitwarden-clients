import { AccountSwitcherEntry } from "./account-switcher-entry.type";

/**
 * The accounts an account switcher shows, split into the active account and the others.
 * A logged-out account appears only as an inactive account that kept its email.
 */
export type AccountSwitcherEntries = {
  /** The active account, or `null` when there is none or it is logged out. */
  active: AccountSwitcherEntry | null;
  /** Every other account, most recently active first. */
  inactive: AccountSwitcherEntry[];
  /** Whether another account can be added without exceeding the account limit. */
  canAddAccount: boolean;
};
