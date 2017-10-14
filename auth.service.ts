import { Injectable } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';
import { AuthHttp, tokenNotExpired } from 'angular2-jwt';
import { HttpService } from './http.service';
import { CurrentUser } from './currentuser.service';
import { Observable } from 'rxjs/Rx';

declare var Auth0: any;
declare var AUTH0_URL: string;
declare let Config: any;

@Injectable()
export class AuthService {
  private auth0: any;
  private redirectUrl: string;

  public constructor(private router: Router, private httpService: HttpService, private currentUser: CurrentUser) {
    this.auth0 = new Auth0({
      domain: "Auth0-Url-Goes-Here",
      clientID: "Auth0-ClientId-Goes-Here",
      callbackURL: window.location.origin,
      responseType: 'token'
    });
    this.redirectUrl = null;
    this.Login();
  }

  public IsAuthenticated(): boolean {
    return tokenNotExpired();
  }

  public Logout(): void {
    localStorage.removeItem('id_token');
    localStorage.removeItem('profile');
    this.router.navigate(["/"]).then(result => { window.location.href = 'https://fs.landmark.co.uk/adfs/ls?wa=wsignout1.0'; });
  }

  public Login(): void {
    // Retrieve Information needed for logon
    var landmarkPayload = this.auth0.parseHash(window.location.hash);
    var isRedirectFromLandmarkADFS = landmarkPayload && landmarkPayload.idToken && landmarkPayload.idTokenPayload;
    var userDetails = localStorage.getItem("profile");
    if (userDetails) {
      this.currentUser.User = JSON.parse(userDetails);
    }
    // Logon Process below involves redirecting users to Auth0 Logon. The payload in the redirect 
    // contains a JWT token AND the users information. We require a users email address from this to log them onto the API. 
    // Follow the four scenarios that could happen below.
    if (!this.IsAuthenticated()) {
      // No Token exists - Not Authenticated
      if (!isRedirectFromLandmarkADFS) {
        // #1 No token exists in local storage and we've just logged on. Redirect user to Auth0/Landmark-ADFS logon.
        this.logonToADFS();
      } else {
        // #2 No token exists in local storage but we've been redirected from Auth0/Landmark-ADFS. Take payload info and log onto Hagrid API.
        localStorage.setItem('id_token', landmarkPayload.idToken);
        this.logonToHagrid(landmarkPayload.idTokenPayload.email, true, landmarkPayload.state);
      }
    } else {
      // Token Exists - Authenticated
      if (this.currentUser.User && this.currentUser.User.Email) {
        // #3 Token AND Profile exist in local storage. Log user onto Hagrid API.
        this.logonToHagrid(this.currentUser.User.Email, false);
      } else {
        // #4 Token but NO Profile exist in local storage. Restart logon process, cannot log user on without email address.
        localStorage.removeItem('id_token');
        localStorage.removeItem('profile');
        this.logonToADFS();
      }
    }
    // Check if something went wrong during login call
    if (landmarkPayload && landmarkPayload.error) {
      throw landmarkPayload.error;
    }
  }

  private logonToADFS(): void {
    // This redirects user to Auth0/LMK-ADFS logon. Which following successful logon will redirect to the user back here.
    this.auth0.login({
      sso: false,
      connection: "Landmark-ADFS",
      scope: 'email openid name picture',
      state: window.location.pathname
    });
  }

  private logonToHagrid(email, showWelcomeMessage, redirectUrl?: string): void {
    // Log user onto Hagrid API and retrieve user details
    this.redirectUrl = redirectUrl;
    this.getLogonUserFromDb(email).subscribe(x => {
      // TODO: Do successful logon things in here.
    });
  }

  private getLogonUserFromDb(email: string): Observable<any> {
    return this.httpService.CallApiGetObservable(`${Config.API_URL}/api/auth/logon?email=${encodeURIComponent(email)}`);
  }
}
