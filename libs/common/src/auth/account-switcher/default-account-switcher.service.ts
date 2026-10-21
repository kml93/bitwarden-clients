import {
  combineLatest,
  defaultIfEmpty,
  firstValueFrom,
  map,
  Observable,
  of,
  switchMap,
} from "rxjs";

import { EnvironmentService } from "../../platform/abstractions/environment.service";
import { Account, AccountService } from "../abstractions/account.service";
import { AuthService } from "../abstractions/auth.service";
import { AvatarService } from "../abstractions/avatar.service";
import { AuthenticationStatus } from "../enums/authentication-status";

import { AccountSwitcherEntries } from "./account-switcher-entries.type";
import { AccountSwitcherEntry } from "./account-switcher-entry.type";
import { AccountSwitcherService } from "./account-switcher.service";
import { ActiveAccountResolution } from "./active-account-resolution.type";

/** The most accounts that can be not logged out at the same time. */
const MAX_ACCOUNTS = 5;

type KnownAccount = { account: Account; status: AuthenticationStatus; isActive: boolean };

const isUsable = (knownAccount: KnownAccount) =>
  knownAccount.status !== AuthenticationStatus.LoggedOut;

/**
 * A logged-out account stays listed while it keeps its email, so the user can pick it to log back
 * in. An account logged out by the "Log out" timeout action keeps its email across restarts; an
 * explicit logout clears it. The active logged-out account is the one being logged into, so it is
 * not listed.
 */
const isListed = (knownAccount: KnownAccount) =>
  isUsable(knownAccount) || (!knownAccount.isActive && !!knownAccount.account.email);

const hasRoomForAnotherAccount = (knownAccounts: KnownAccount[]) =>
  knownAccounts.filter(isUsable).length < MAX_ACCOUNTS;

export class DefaultAccountSwitcherService implements AccountSwitcherService {
  readonly entries$: Observable<AccountSwitcherEntries>;
  readonly canAddAccount$: Observable<boolean>;
  readonly nextSwitchableAccount$: Observable<Account | null>;

  /** Every account that is not logged out, the active account included, most recently active first. */
  private readonly usableAccounts$: Observable<KnownAccount[]>;

  constructor(
    private readonly accountService: AccountService,
    private readonly authService: AuthService,
    private readonly avatarService: AvatarService,
    private readonly environmentService: EnvironmentService,
  ) {
    const knownAccounts$: Observable<KnownAccount[]> = combineLatest({
      accounts: this.accountService.accounts$,
      activeAccount: this.accountService.activeAccount$,
      sortedUserIds: this.accountService.sortedUserIds$,
      authStatuses: this.authService.authStatuses$,
    }).pipe(
      map(({ accounts, activeAccount, sortedUserIds, authStatuses }) =>
        sortedUserIds
          // Keep known accounts with a known status.
          .filter((userId) => accounts[userId] != null && authStatuses[userId] != null)
          .map((userId) => ({
            account: { id: userId, ...accounts[userId] },
            status: authStatuses[userId],
            isActive: userId === activeAccount?.id,
          })),
      ),
    );

    this.usableAccounts$ = knownAccounts$.pipe(
      map((knownAccounts) => knownAccounts.filter(isUsable)),
    );

    this.entries$ = knownAccounts$.pipe(
      switchMap((knownAccounts) => this.toEntries$(knownAccounts.filter(isListed))),
    );

    this.canAddAccount$ = this.usableAccounts$.pipe(map(hasRoomForAnotherAccount));

    this.nextSwitchableAccount$ = this.usableAccounts$.pipe(
      map(
        (usableAccounts) =>
          usableAccounts.find((usableAccount) => !usableAccount.isActive)?.account ?? null,
      ),
    );
  }

  async resolveActiveAccount(): Promise<ActiveAccountResolution> {
    const activeAccount = await firstValueFrom(this.accountService.activeAccount$);
    if (activeAccount == null) {
      return { action: "keep" };
    }

    const authStatus = await firstValueFrom(this.authService.authStatusFor$(activeAccount.id));
    if (authStatus !== AuthenticationStatus.LoggedOut) {
      return { action: "keep" };
    }

    const nextAccount = await firstValueFrom(this.nextSwitchableAccount$);
    return nextAccount == null
      ? { action: "clear" }
      : { action: "switch", targetUserId: nextAccount.id };
  }

  private toEntries$(listedAccounts: KnownAccount[]): Observable<AccountSwitcherEntries> {
    const activeListedAccount = listedAccounts.find((listedAccount) => listedAccount.isActive);
    const inactiveListedAccounts = listedAccounts.filter(
      (listedAccount) => !listedAccount.isActive,
    );

    const activeEntry$ =
      activeListedAccount == null ? of(null) : this.toEntry$(activeListedAccount);
    const inactiveEntries$ = combineLatest(
      inactiveListedAccounts.map((inactiveListedAccount) => this.toEntry$(inactiveListedAccount)),
    ).pipe(
      // With no inactive accounts, combineLatest completes without a value. The combined result
      // below needs a value from every source before it emits.
      defaultIfEmpty([]),
    );

    return combineLatest({ active: activeEntry$, inactive: inactiveEntries$ }).pipe(
      map((entries) => ({ ...entries, canAddAccount: hasRoomForAnotherAccount(listedAccounts) })),
    );
  }

  private toEntry$({ account, status }: KnownAccount): Observable<AccountSwitcherEntry> {
    return combineLatest([
      this.avatarService.getUserAvatarColor$(account.id),
      this.environmentService.getEnvironment$(account.id),
    ]).pipe(
      map(([avatarColor, environment]) => ({
        id: account.id,
        name: account.name,
        email: account.email,
        status,
        avatarColor,
        serverHostname: environment?.getHostname(),
      })),
    );
  }
}
