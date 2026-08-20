(function(){const e=document.createElement("link").relList;if(e&&e.supports&&e.supports("modulepreload"))return;for(const s of document.querySelectorAll('link[rel="modulepreload"]'))a(s);new MutationObserver(s=>{for(const n of s)if(n.type==="childList")for(const r of n.addedNodes)r.tagName==="LINK"&&r.rel==="modulepreload"&&a(r)}).observe(document,{childList:!0,subtree:!0});function i(s){const n={};return s.integrity&&(n.integrity=s.integrity),s.referrerPolicy&&(n.referrerPolicy=s.referrerPolicy),s.crossOrigin==="use-credentials"?n.credentials="include":s.crossOrigin==="anonymous"?n.credentials="omit":n.credentials="same-origin",n}function a(s){if(s.ep)return;s.ep=!0;const n=i(s);fetch(s.href,n)}})();const Ae=globalThis,at=Ae.ShadowRoot&&(Ae.ShadyCSS===void 0||Ae.ShadyCSS.nativeShadow)&&"adoptedStyleSheets"in Document.prototype&&"replace"in CSSStyleSheet.prototype,rt=Symbol(),ft=new WeakMap;let Qt=class{constructor(e,i,a){if(this._$cssResult$=!0,a!==rt)throw Error("CSSResult is not constructable. Use `unsafeCSS` or `css` instead.");this.cssText=e,this.t=i}get styleSheet(){let e=this.o;const i=this.t;if(at&&e===void 0){const a=i!==void 0&&i.length===1;a&&(e=ft.get(i)),e===void 0&&((this.o=e=new CSSStyleSheet).replaceSync(this.cssText),a&&ft.set(i,e))}return e}toString(){return this.cssText}};const ji=t=>new Qt(typeof t=="string"?t:t+"",void 0,rt),F=(t,...e)=>{const i=t.length===1?t[0]:e.reduce((a,s,n)=>a+(r=>{if(r._$cssResult$===!0)return r.cssText;if(typeof r=="number")return r;throw Error("Value passed to 'css' function must be a 'css' function result: "+r+". Use 'unsafeCSS' to pass non-literal values, but take care to ensure page security.")})(s)+t[n+1],t[0]);return new Qt(i,t,rt)},Li=(t,e)=>{if(at)t.adoptedStyleSheets=e.map(i=>i instanceof CSSStyleSheet?i:i.styleSheet);else for(const i of e){const a=document.createElement("style"),s=Ae.litNonce;s!==void 0&&a.setAttribute("nonce",s),a.textContent=i.cssText,t.appendChild(a)}},bt=at?t=>t:t=>t instanceof CSSStyleSheet?(e=>{let i="";for(const a of e.cssRules)i+=a.cssText;return ji(i)})(t):t;const{is:Mi,defineProperty:Ni,getOwnPropertyDescriptor:Ri,getOwnPropertyNames:Ui,getOwnPropertySymbols:qi,getPrototypeOf:Fi}=Object,Le=globalThis,vt=Le.trustedTypes,zi=vt?vt.emptyScript:"",Hi=Le.reactiveElementPolyfillSupport,he=(t,e)=>t,Pe={toAttribute(t,e){switch(e){case Boolean:t=t?zi:null;break;case Object:case Array:t=t==null?t:JSON.stringify(t)}return t},fromAttribute(t,e){let i=t;switch(e){case Boolean:i=t!==null;break;case Number:i=t===null?null:Number(t);break;case Object:case Array:try{i=JSON.parse(t)}catch{i=null}}return i}},nt=(t,e)=>!Mi(t,e),yt={attribute:!0,type:String,converter:Pe,reflect:!1,useDefault:!1,hasChanged:nt};Symbol.metadata??=Symbol("metadata"),Le.litPropertyMetadata??=new WeakMap;let ae=class extends HTMLElement{static addInitializer(e){this._$Ei(),(this.l??=[]).push(e)}static get observedAttributes(){return this.finalize(),this._$Eh&&[...this._$Eh.keys()]}static createProperty(e,i=yt){if(i.state&&(i.attribute=!1),this._$Ei(),this.prototype.hasOwnProperty(e)&&((i=Object.create(i)).wrapped=!0),this.elementProperties.set(e,i),!i.noAccessor){const a=Symbol(),s=this.getPropertyDescriptor(e,a,i);s!==void 0&&Ni(this.prototype,e,s)}}static getPropertyDescriptor(e,i,a){const{get:s,set:n}=Ri(this.prototype,e)??{get(){return this[i]},set(r){this[i]=r}};return{get:s,set(r){const o=s?.call(this);n?.call(this,r),this.requestUpdate(e,o,a)},configurable:!0,enumerable:!0}}static getPropertyOptions(e){return this.elementProperties.get(e)??yt}static _$Ei(){if(this.hasOwnProperty(he("elementProperties")))return;const e=Fi(this);e.finalize(),e.l!==void 0&&(this.l=[...e.l]),this.elementProperties=new Map(e.elementProperties)}static finalize(){if(this.hasOwnProperty(he("finalized")))return;if(this.finalized=!0,this._$Ei(),this.hasOwnProperty(he("properties"))){const i=this.properties,a=[...Ui(i),...qi(i)];for(const s of a)this.createProperty(s,i[s])}const e=this[Symbol.metadata];if(e!==null){const i=litPropertyMetadata.get(e);if(i!==void 0)for(const[a,s]of i)this.elementProperties.set(a,s)}this._$Eh=new Map;for(const[i,a]of this.elementProperties){const s=this._$Eu(i,a);s!==void 0&&this._$Eh.set(s,i)}this.elementStyles=this.finalizeStyles(this.styles)}static finalizeStyles(e){const i=[];if(Array.isArray(e)){const a=new Set(e.flat(1/0).reverse());for(const s of a)i.unshift(bt(s))}else e!==void 0&&i.push(bt(e));return i}static _$Eu(e,i){const a=i.attribute;return a===!1?void 0:typeof a=="string"?a:typeof e=="string"?e.toLowerCase():void 0}constructor(){super(),this._$Ep=void 0,this.isUpdatePending=!1,this.hasUpdated=!1,this._$Em=null,this._$Ev()}_$Ev(){this._$ES=new Promise(e=>this.enableUpdating=e),this._$AL=new Map,this._$E_(),this.requestUpdate(),this.constructor.l?.forEach(e=>e(this))}addController(e){(this._$EO??=new Set).add(e),this.renderRoot!==void 0&&this.isConnected&&e.hostConnected?.()}removeController(e){this._$EO?.delete(e)}_$E_(){const e=new Map,i=this.constructor.elementProperties;for(const a of i.keys())this.hasOwnProperty(a)&&(e.set(a,this[a]),delete this[a]);e.size>0&&(this._$Ep=e)}createRenderRoot(){const e=this.shadowRoot??this.attachShadow(this.constructor.shadowRootOptions);return Li(e,this.constructor.elementStyles),e}connectedCallback(){this.renderRoot??=this.createRenderRoot(),this.enableUpdating(!0),this._$EO?.forEach(e=>e.hostConnected?.())}enableUpdating(e){}disconnectedCallback(){this._$EO?.forEach(e=>e.hostDisconnected?.())}attributeChangedCallback(e,i,a){this._$AK(e,a)}_$ET(e,i){const a=this.constructor.elementProperties.get(e),s=this.constructor._$Eu(e,a);if(s!==void 0&&a.reflect===!0){const n=(a.converter?.toAttribute!==void 0?a.converter:Pe).toAttribute(i,a.type);this._$Em=e,n==null?this.removeAttribute(s):this.setAttribute(s,n),this._$Em=null}}_$AK(e,i){const a=this.constructor,s=a._$Eh.get(e);if(s!==void 0&&this._$Em!==s){const n=a.getPropertyOptions(s),r=typeof n.converter=="function"?{fromAttribute:n.converter}:n.converter?.fromAttribute!==void 0?n.converter:Pe;this._$Em=s;const o=r.fromAttribute(i,n.type);this[s]=o??this._$Ej?.get(s)??o,this._$Em=null}}requestUpdate(e,i,a,s=!1,n){if(e!==void 0){const r=this.constructor;if(s===!1&&(n=this[e]),a??=r.getPropertyOptions(e),!((a.hasChanged??nt)(n,i)||a.useDefault&&a.reflect&&n===this._$Ej?.get(e)&&!this.hasAttribute(r._$Eu(e,a))))return;this.C(e,i,a)}this.isUpdatePending===!1&&(this._$ES=this._$EP())}C(e,i,{useDefault:a,reflect:s,wrapped:n},r){a&&!(this._$Ej??=new Map).has(e)&&(this._$Ej.set(e,r??i??this[e]),n!==!0||r!==void 0)||(this._$AL.has(e)||(this.hasUpdated||a||(i=void 0),this._$AL.set(e,i)),s===!0&&this._$Em!==e&&(this._$Eq??=new Set).add(e))}async _$EP(){this.isUpdatePending=!0;try{await this._$ES}catch(i){Promise.reject(i)}const e=this.scheduleUpdate();return e!=null&&await e,!this.isUpdatePending}scheduleUpdate(){return this.performUpdate()}performUpdate(){if(!this.isUpdatePending)return;if(!this.hasUpdated){if(this.renderRoot??=this.createRenderRoot(),this._$Ep){for(const[s,n]of this._$Ep)this[s]=n;this._$Ep=void 0}const a=this.constructor.elementProperties;if(a.size>0)for(const[s,n]of a){const{wrapped:r}=n,o=this[s];r!==!0||this._$AL.has(s)||o===void 0||this.C(s,void 0,n,o)}}let e=!1;const i=this._$AL;try{e=this.shouldUpdate(i),e?(this.willUpdate(i),this._$EO?.forEach(a=>a.hostUpdate?.()),this.update(i)):this._$EM()}catch(a){throw e=!1,this._$EM(),a}e&&this._$AE(i)}willUpdate(e){}_$AE(e){this._$EO?.forEach(i=>i.hostUpdated?.()),this.hasUpdated||(this.hasUpdated=!0,this.firstUpdated(e)),this.updated(e)}_$EM(){this._$AL=new Map,this.isUpdatePending=!1}get updateComplete(){return this.getUpdateComplete()}getUpdateComplete(){return this._$ES}shouldUpdate(e){return!0}update(e){this._$Eq&&=this._$Eq.forEach(i=>this._$ET(i,this[i])),this._$EM()}updated(e){}firstUpdated(e){}};ae.elementStyles=[],ae.shadowRootOptions={mode:"open"},ae[he("elementProperties")]=new Map,ae[he("finalized")]=new Map,Hi?.({ReactiveElement:ae}),(Le.reactiveElementVersions??=[]).push("2.1.2");const ot=globalThis,xt=t=>t,De=ot.trustedTypes,$t=De?De.createPolicy("lit-html",{createHTML:t=>t}):void 0,Yt="$lit$",H=`lit$${Math.random().toFixed(9).slice(2)}$`,Zt="?"+H,Bi=`<${Zt}>`,Z=document,me=()=>Z.createComment(""),fe=t=>t===null||typeof t!="object"&&typeof t!="function",lt=Array.isArray,Vi=t=>lt(t)||typeof t?.[Symbol.iterator]=="function",qe=`[ 	
\f\r]`,ce=/<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g,wt=/-->/g,kt=/>/g,G=RegExp(`>|${qe}(?:([^\\s"'>=/]+)(${qe}*=${qe}*(?:[^ 	
\f\r"'\`<>=]|("|')|))|$)`,"g"),_t=/'/g,St=/"/g,Xt=/^(?:script|style|textarea|title)$/i,Ji=t=>(e,...i)=>({_$litType$:t,strings:e,values:i}),c=Ji(1),X=Symbol.for("lit-noChange"),h=Symbol.for("lit-nothing"),At=new WeakMap,Q=Z.createTreeWalker(Z,129);function ei(t,e){if(!lt(t)||!t.hasOwnProperty("raw"))throw Error("invalid template strings array");return $t!==void 0?$t.createHTML(e):e}const Gi=(t,e)=>{const i=t.length-1,a=[];let s,n=e===2?"<svg>":e===3?"<math>":"",r=ce;for(let o=0;o<i;o++){const l=t[o];let d,p,u=-1,m=0;for(;m<l.length&&(r.lastIndex=m,p=r.exec(l),p!==null);)m=r.lastIndex,r===ce?p[1]==="!--"?r=wt:p[1]!==void 0?r=kt:p[2]!==void 0?(Xt.test(p[2])&&(s=RegExp("</"+p[2],"g")),r=G):p[3]!==void 0&&(r=G):r===G?p[0]===">"?(r=s??ce,u=-1):p[1]===void 0?u=-2:(u=r.lastIndex-p[2].length,d=p[1],r=p[3]===void 0?G:p[3]==='"'?St:_t):r===St||r===_t?r=G:r===wt||r===kt?r=ce:(r=G,s=void 0);const b=r===G&&t[o+1].startsWith("/>")?" ":"";n+=r===ce?l+Bi:u>=0?(a.push(d),l.slice(0,u)+Yt+l.slice(u)+H+b):l+H+(u===-2?o:b)}return[ei(t,n+(t[i]||"<?>")+(e===2?"</svg>":e===3?"</math>":"")),a]};class be{constructor({strings:e,_$litType$:i},a){let s;this.parts=[];let n=0,r=0;const o=e.length-1,l=this.parts,[d,p]=Gi(e,i);if(this.el=be.createElement(d,a),Q.currentNode=this.el.content,i===2||i===3){const u=this.el.content.firstChild;u.replaceWith(...u.childNodes)}for(;(s=Q.nextNode())!==null&&l.length<o;){if(s.nodeType===1){if(s.hasAttributes())for(const u of s.getAttributeNames())if(u.endsWith(Yt)){const m=p[r++],b=s.getAttribute(u).split(H),$=/([.?@])?(.*)/.exec(m);l.push({type:1,index:n,name:$[2],strings:b,ctor:$[1]==="."?Wi:$[1]==="?"?Qi:$[1]==="@"?Yi:Me}),s.removeAttribute(u)}else u.startsWith(H)&&(l.push({type:6,index:n}),s.removeAttribute(u));if(Xt.test(s.tagName)){const u=s.textContent.split(H),m=u.length-1;if(m>0){s.textContent=De?De.emptyScript:"";for(let b=0;b<m;b++)s.append(u[b],me()),Q.nextNode(),l.push({type:2,index:++n});s.append(u[m],me())}}}else if(s.nodeType===8)if(s.data===Zt)l.push({type:2,index:n});else{let u=-1;for(;(u=s.data.indexOf(H,u+1))!==-1;)l.push({type:7,index:n}),u+=H.length-1}n++}}static createElement(e,i){const a=Z.createElement("template");return a.innerHTML=e,a}}function re(t,e,i=t,a){if(e===X)return e;let s=a!==void 0?i._$Co?.[a]:i._$Cl;const n=fe(e)?void 0:e._$litDirective$;return s?.constructor!==n&&(s?._$AO?.(!1),n===void 0?s=void 0:(s=new n(t),s._$AT(t,i,a)),a!==void 0?(i._$Co??=[])[a]=s:i._$Cl=s),s!==void 0&&(e=re(t,s._$AS(t,e.values),s,a)),e}class Ki{constructor(e,i){this._$AV=[],this._$AN=void 0,this._$AD=e,this._$AM=i}get parentNode(){return this._$AM.parentNode}get _$AU(){return this._$AM._$AU}u(e){const{el:{content:i},parts:a}=this._$AD,s=(e?.creationScope??Z).importNode(i,!0);Q.currentNode=s;let n=Q.nextNode(),r=0,o=0,l=a[0];for(;l!==void 0;){if(r===l.index){let d;l.type===2?d=new oe(n,n.nextSibling,this,e):l.type===1?d=new l.ctor(n,l.name,l.strings,this,e):l.type===6&&(d=new Zi(n,this,e)),this._$AV.push(d),l=a[++o]}r!==l?.index&&(n=Q.nextNode(),r++)}return Q.currentNode=Z,s}p(e){let i=0;for(const a of this._$AV)a!==void 0&&(a.strings!==void 0?(a._$AI(e,a,i),i+=a.strings.length-2):a._$AI(e[i])),i++}}class oe{get _$AU(){return this._$AM?._$AU??this._$Cv}constructor(e,i,a,s){this.type=2,this._$AH=h,this._$AN=void 0,this._$AA=e,this._$AB=i,this._$AM=a,this.options=s,this._$Cv=s?.isConnected??!0}get parentNode(){let e=this._$AA.parentNode;const i=this._$AM;return i!==void 0&&e?.nodeType===11&&(e=i.parentNode),e}get startNode(){return this._$AA}get endNode(){return this._$AB}_$AI(e,i=this){e=re(this,e,i),fe(e)?e===h||e==null||e===""?(this._$AH!==h&&this._$AR(),this._$AH=h):e!==this._$AH&&e!==X&&this._(e):e._$litType$!==void 0?this.$(e):e.nodeType!==void 0?this.T(e):Vi(e)?this.k(e):this._(e)}O(e){return this._$AA.parentNode.insertBefore(e,this._$AB)}T(e){this._$AH!==e&&(this._$AR(),this._$AH=this.O(e))}_(e){this._$AH!==h&&fe(this._$AH)?this._$AA.nextSibling.data=e:this.T(Z.createTextNode(e)),this._$AH=e}$(e){const{values:i,_$litType$:a}=e,s=typeof a=="number"?this._$AC(e):(a.el===void 0&&(a.el=be.createElement(ei(a.h,a.h[0]),this.options)),a);if(this._$AH?._$AD===s)this._$AH.p(i);else{const n=new Ki(s,this),r=n.u(this.options);n.p(i),this.T(r),this._$AH=n}}_$AC(e){let i=At.get(e.strings);return i===void 0&&At.set(e.strings,i=new be(e)),i}k(e){lt(this._$AH)||(this._$AH=[],this._$AR());const i=this._$AH;let a,s=0;for(const n of e)s===i.length?i.push(a=new oe(this.O(me()),this.O(me()),this,this.options)):a=i[s],a._$AI(n),s++;s<i.length&&(this._$AR(a&&a._$AB.nextSibling,s),i.length=s)}_$AR(e=this._$AA.nextSibling,i){for(this._$AP?.(!1,!0,i);e!==this._$AB;){const a=xt(e).nextSibling;xt(e).remove(),e=a}}setConnected(e){this._$AM===void 0&&(this._$Cv=e,this._$AP?.(e))}}class Me{get tagName(){return this.element.tagName}get _$AU(){return this._$AM._$AU}constructor(e,i,a,s,n){this.type=1,this._$AH=h,this._$AN=void 0,this.element=e,this.name=i,this._$AM=s,this.options=n,a.length>2||a[0]!==""||a[1]!==""?(this._$AH=Array(a.length-1).fill(new String),this.strings=a):this._$AH=h}_$AI(e,i=this,a,s){const n=this.strings;let r=!1;if(n===void 0)e=re(this,e,i,0),r=!fe(e)||e!==this._$AH&&e!==X,r&&(this._$AH=e);else{const o=e;let l,d;for(e=n[0],l=0;l<n.length-1;l++)d=re(this,o[a+l],i,l),d===X&&(d=this._$AH[l]),r||=!fe(d)||d!==this._$AH[l],d===h?e=h:e!==h&&(e+=(d??"")+n[l+1]),this._$AH[l]=d}r&&!s&&this.j(e)}j(e){e===h?this.element.removeAttribute(this.name):this.element.setAttribute(this.name,e??"")}}class Wi extends Me{constructor(){super(...arguments),this.type=3}j(e){this.element[this.name]=e===h?void 0:e}}class Qi extends Me{constructor(){super(...arguments),this.type=4}j(e){this.element.toggleAttribute(this.name,!!e&&e!==h)}}class Yi extends Me{constructor(e,i,a,s,n){super(e,i,a,s,n),this.type=5}_$AI(e,i=this){if((e=re(this,e,i,0)??h)===X)return;const a=this._$AH,s=e===h&&a!==h||e.capture!==a.capture||e.once!==a.once||e.passive!==a.passive,n=e!==h&&(a===h||s);s&&this.element.removeEventListener(this.name,this,a),n&&this.element.addEventListener(this.name,this,e),this._$AH=e}handleEvent(e){typeof this._$AH=="function"?this._$AH.call(this.options?.host??this.element,e):this._$AH.handleEvent(e)}}class Zi{constructor(e,i,a){this.element=e,this.type=6,this._$AN=void 0,this._$AM=i,this.options=a}get _$AU(){return this._$AM._$AU}_$AI(e){re(this,e)}}const Xi={I:oe},es=ot.litHtmlPolyfillSupport;es?.(be,oe),(ot.litHtmlVersions??=[]).push("3.3.3");const ts=(t,e,i)=>{const a=i?.renderBefore??e;let s=a._$litPart$;if(s===void 0){const n=i?.renderBefore??null;a._$litPart$=s=new oe(e.insertBefore(me(),n),n,void 0,i??{})}return s._$AI(t),s};const ct=globalThis;let L=class extends ae{constructor(){super(...arguments),this.renderOptions={host:this},this._$Do=void 0}createRenderRoot(){const e=super.createRenderRoot();return this.renderOptions.renderBefore??=e.firstChild,e}update(e){const i=this.render();this.hasUpdated||(this.renderOptions.isConnected=this.isConnected),super.update(e),this._$Do=ts(i,this.renderRoot,this.renderOptions)}connectedCallback(){super.connectedCallback(),this._$Do?.setConnected(!0)}disconnectedCallback(){super.disconnectedCallback(),this._$Do?.setConnected(!1)}render(){return X}};L._$litElement$=!0,L.finalized=!0,ct.litElementHydrateSupport?.({LitElement:L});const is=ct.litElementPolyfillSupport;is?.({LitElement:L});(ct.litElementVersions??=[]).push("4.2.2");const ie=t=>(e,i)=>{i!==void 0?i.addInitializer(()=>{customElements.define(t,e)}):customElements.define(t,e)};const ss={attribute:!0,type:String,converter:Pe,reflect:!1,hasChanged:nt},as=(t=ss,e,i)=>{const{kind:a,metadata:s}=i;let n=globalThis.litPropertyMetadata.get(s);if(n===void 0&&globalThis.litPropertyMetadata.set(s,n=new Map),a==="setter"&&((t=Object.create(t)).wrapped=!0),n.set(i.name,t),a==="accessor"){const{name:r}=i;return{set(o){const l=e.get.call(this);e.set.call(this,o),this.requestUpdate(r,l,t,!0,o)},init(o){return o!==void 0&&this.C(r,void 0,t,o),o}}}if(a==="setter"){const{name:r}=i;return function(o){const l=this[r];e.call(this,o),this.requestUpdate(r,l,t,!0,o)}}throw Error("Unsupported decorator location: "+a)};function S(t){return(e,i)=>typeof i=="object"?as(t,e,i):((a,s,n)=>{const r=s.hasOwnProperty(n);return s.constructor.createProperty(n,a),r?Object.getOwnPropertyDescriptor(s,n):void 0})(t,e,i)}function g(t){return S({...t,state:!0,attribute:!1})}const ti={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 4h4v16H6zm8 0h4v16h-4z"/>'},ii={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m5 3l14 9l-14 9V3z"/>'},ne={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 6h18m-2 0v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6m3 0V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2m-6 5v6m4-6v6"/>'},si={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18 6L6 18M6 6l12 12"/>'},rs={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><path d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></g>'};const ai=Object.freeze({left:0,top:0,width:16,height:16}),Ie=Object.freeze({rotate:0,vFlip:!1,hFlip:!1}),xe=Object.freeze({...ai,...Ie}),Ge=Object.freeze({...xe,body:"",hidden:!1}),ns=Object.freeze({width:null,height:null}),ri=Object.freeze({...ns,...Ie});function os(t,e=0){const i=t.replace(/^-?[0-9.]*/,"");function a(s){for(;s<0;)s+=4;return s%4}if(i===""){const s=parseInt(t);return isNaN(s)?0:a(s)}else if(i!==t){let s=0;switch(i){case"%":s=25;break;case"deg":s=90}if(s){let n=parseFloat(t.slice(0,t.length-i.length));return isNaN(n)?0:(n=n/s,n%1===0?a(n):0)}}return e}const ls=/[\s,]+/;function cs(t,e){e.split(ls).forEach(i=>{switch(i.trim()){case"horizontal":t.hFlip=!0;break;case"vertical":t.vFlip=!0;break}})}const ni={...ri,preserveAspectRatio:""};function Tt(t){const e={...ni},i=(a,s)=>t.getAttribute(a)||s;return e.width=i("width",null),e.height=i("height",null),e.rotate=os(i("rotate","")),cs(e,i("flip","")),e.preserveAspectRatio=i("preserveAspectRatio",i("preserveaspectratio","")),e}function ds(t,e){for(const i in ni)if(t[i]!==e[i])return!0;return!1}const oi=/^[a-z0-9]+(-[a-z0-9]+)*$/,$e=(t,e,i,a="")=>{const s=t.split(":");if(t.slice(0,1)==="@"){if(s.length<2||s.length>3)return null;a=s.shift().slice(1)}if(s.length>3||!s.length)return null;if(s.length>1){const o=s.pop(),l=s.pop(),d={provider:s.length>0?s[0]:a,prefix:l,name:o};return e&&!Te(d)?null:d}const n=s[0],r=n.split("-");if(r.length>1){const o={provider:a,prefix:r.shift(),name:r.join("-")};return e&&!Te(o)?null:o}if(i&&a===""){const o={provider:a,prefix:"",name:n};return e&&!Te(o,i)?null:o}return null},Te=(t,e)=>t?!!((e&&t.prefix===""||t.prefix)&&t.name):!1;function us(t,e){const i=t.icons,a=t.aliases||Object.create(null),s=Object.create(null);function n(r){if(i[r])return s[r]=[];if(!(r in s)){s[r]=null;const o=a[r]&&a[r].parent,l=o&&n(o);l&&(s[r]=[o].concat(l))}return s[r]}return Object.keys(i).concat(Object.keys(a)).forEach(n),s}function ps(t,e){const i={};!t.hFlip!=!e.hFlip&&(i.hFlip=!0),!t.vFlip!=!e.vFlip&&(i.vFlip=!0);const a=((t.rotate||0)+(e.rotate||0))%4;return a&&(i.rotate=a),i}function Ct(t,e){const i=ps(t,e);for(const a in Ge)a in Ie?a in t&&!(a in i)&&(i[a]=Ie[a]):a in e?i[a]=e[a]:a in t&&(i[a]=t[a]);return i}function hs(t,e,i){const a=t.icons,s=t.aliases||Object.create(null);let n={};function r(o){n=Ct(a[o]||s[o],n)}return r(e),i.forEach(r),Ct(t,n)}function li(t,e){const i=[];if(typeof t!="object"||typeof t.icons!="object")return i;t.not_found instanceof Array&&t.not_found.forEach(s=>{e(s,null),i.push(s)});const a=us(t);for(const s in a){const n=a[s];n&&(e(s,hs(t,s,n)),i.push(s))}return i}const gs={provider:"",aliases:{},not_found:{},...ai};function Fe(t,e){for(const i in e)if(i in t&&typeof t[i]!=typeof e[i])return!1;return!0}function ci(t){if(typeof t!="object"||t===null)return null;const e=t;if(typeof e.prefix!="string"||!t.icons||typeof t.icons!="object"||!Fe(t,gs))return null;const i=e.icons;for(const s in i){const n=i[s];if(!s||typeof n.body!="string"||!Fe(n,Ge))return null}const a=e.aliases||Object.create(null);for(const s in a){const n=a[s],r=n.parent;if(!s||typeof r!="string"||!i[r]&&!a[r]||!Fe(n,Ge))return null}return e}const Oe=Object.create(null);function ms(t,e){return{provider:t,prefix:e,icons:Object.create(null),missing:new Set}}function U(t,e){const i=Oe[t]||(Oe[t]=Object.create(null));return i[e]||(i[e]=ms(t,e))}function di(t,e){return ci(e)?li(e,(i,a)=>{a?t.icons[i]=a:t.missing.add(i)}):[]}function fs(t,e,i){try{if(typeof i.body=="string")return t.icons[e]={...i},!0}catch{}return!1}function bs(t,e){let i=[];return(typeof t=="string"?[t]:Object.keys(Oe)).forEach(a=>{(typeof a=="string"&&typeof e=="string"?[e]:Object.keys(Oe[a]||{})).forEach(s=>{const n=U(a,s);i=i.concat(Object.keys(n.icons).map(r=>(a!==""?"@"+a+":":"")+s+":"+r))})}),i}let ve=!1;function ui(t){return typeof t=="boolean"&&(ve=t),ve}function ye(t){const e=typeof t=="string"?$e(t,!0,ve):t;if(e){const i=U(e.provider,e.prefix),a=e.name;return i.icons[a]||(i.missing.has(a)?null:void 0)}}function pi(t,e){const i=$e(t,!0,ve);if(!i)return!1;const a=U(i.provider,i.prefix);return e?fs(a,i.name,e):(a.missing.add(i.name),!0)}function Et(t,e){if(typeof t!="object")return!1;if(typeof e!="string"&&(e=t.provider||""),ve&&!e&&!t.prefix){let a=!1;return ci(t)&&(t.prefix="",li(t,(s,n)=>{pi(s,n)&&(a=!0)})),a}const i=t.prefix;return Te({prefix:i,name:"a"})?!!di(U(e,i),t):!1}function vs(t){return!!ye(t)}function ys(t){const e=ye(t);return e&&{...xe,...e}}function hi(t,e){t.forEach(i=>{const a=i.loaderCallbacks;a&&(i.loaderCallbacks=a.filter(s=>s.id!==e))})}function xs(t){t.pendingCallbacksFlag||(t.pendingCallbacksFlag=!0,setTimeout(()=>{t.pendingCallbacksFlag=!1;const e=t.loaderCallbacks?t.loaderCallbacks.slice(0):[];if(!e.length)return;let i=!1;const a=t.provider,s=t.prefix;e.forEach(n=>{const r=n.icons,o=r.pending.length;r.pending=r.pending.filter(l=>{if(l.prefix!==s)return!0;const d=l.name;if(t.icons[d])r.loaded.push({provider:a,prefix:s,name:d});else if(t.missing.has(d))r.missing.push({provider:a,prefix:s,name:d});else return i=!0,!0;return!1}),r.pending.length!==o&&(i||hi([t],n.id),n.callback(r.loaded.slice(0),r.missing.slice(0),r.pending.slice(0),n.abort))})}))}let $s=0;function ws(t,e,i){const a=$s++,s=hi.bind(null,i,a);if(!e.pending.length)return s;const n={id:a,icons:e,callback:t,abort:s};return i.forEach(r=>{(r.loaderCallbacks||(r.loaderCallbacks=[])).push(n)}),s}function ks(t){const e={loaded:[],missing:[],pending:[]},i=Object.create(null);t.sort((s,n)=>s.provider!==n.provider?s.provider.localeCompare(n.provider):s.prefix!==n.prefix?s.prefix.localeCompare(n.prefix):s.name.localeCompare(n.name));let a={provider:"",prefix:"",name:""};return t.forEach(s=>{if(a.name===s.name&&a.prefix===s.prefix&&a.provider===s.provider)return;a=s;const n=s.provider,r=s.prefix,o=s.name,l=i[n]||(i[n]=Object.create(null)),d=l[r]||(l[r]=U(n,r));let p;o in d.icons?p=e.loaded:r===""||d.missing.has(o)?p=e.missing:p=e.pending;const u={provider:n,prefix:r,name:o};p.push(u)}),e}const Ke=Object.create(null);function Pt(t,e){Ke[t]=e}function We(t){return Ke[t]||Ke[""]}function _s(t,e=!0,i=!1){const a=[];return t.forEach(s=>{const n=typeof s=="string"?$e(s,e,i):s;n&&a.push(n)}),a}function dt(t){let e;if(typeof t.resources=="string")e=[t.resources];else if(e=t.resources,!(e instanceof Array)||!e.length)return null;return{resources:e,path:t.path||"/",maxURL:t.maxURL||500,rotate:t.rotate||750,timeout:t.timeout||5e3,random:t.random===!0,index:t.index||0,dataAfterTimeout:t.dataAfterTimeout!==!1}}const Ne=Object.create(null),de=["https://api.simplesvg.com","https://api.unisvg.com"],Ce=[];for(;de.length>0;)de.length===1||Math.random()>.5?Ce.push(de.shift()):Ce.push(de.pop());Ne[""]=dt({resources:["https://api.iconify.design"].concat(Ce)});function Dt(t,e){const i=dt(e);return i===null?!1:(Ne[t]=i,!0)}function Re(t){return Ne[t]}function Ss(){return Object.keys(Ne)}const As={resources:[],index:0,timeout:2e3,rotate:750,random:!1,dataAfterTimeout:!1};function Ts(t,e,i,a){const s=t.resources.length,n=t.random?Math.floor(Math.random()*s):t.index;let r;if(t.random){let w=t.resources.slice(0);for(r=[];w.length>1;){const A=Math.floor(Math.random()*w.length);r.push(w[A]),w=w.slice(0,A).concat(w.slice(A+1))}r=r.concat(w)}else r=t.resources.slice(n).concat(t.resources.slice(0,n));const o=Date.now();let l="pending",d=0,p,u=null,m=[],b=[];typeof a=="function"&&b.push(a);function $(){u&&(clearTimeout(u),u=null)}function k(){l==="pending"&&(l="aborted"),$(),m.forEach(w=>{w.status==="pending"&&(w.status="aborted")}),m=[]}function _(w,A){A&&(b=[]),typeof w=="function"&&b.push(w)}function I(){return{startTime:o,payload:e,status:l,queriesSent:d,queriesPending:m.length,subscribe:_,abort:k}}function N(){l="failed",b.forEach(w=>{w(void 0,p)})}function O(){m.forEach(w=>{w.status==="pending"&&(w.status="aborted")}),m=[]}function E(w,A,V){const J=A!=="success";switch(m=m.filter(C=>C!==w),l){case"pending":break;case"failed":if(J||!t.dataAfterTimeout)return;break;default:return}if(A==="abort"){p=V,N();return}if(J){p=V,m.length||(r.length?le():N());return}if($(),O(),!t.random){const C=t.resources.indexOf(w.resource);C!==-1&&C!==t.index&&(t.index=C)}l="completed",b.forEach(C=>{C(V)})}function le(){if(l!=="pending")return;$();const w=r.shift();if(w===void 0){if(m.length){u=setTimeout(()=>{$(),l==="pending"&&(O(),N())},t.timeout);return}N();return}const A={status:"pending",resource:w,callback:(V,J)=>{E(A,V,J)}};m.push(A),d++,u=setTimeout(le,t.rotate),i(w,e,A.callback)}return setTimeout(le),I}function gi(t){const e={...As,...t};let i=[];function a(){i=i.filter(r=>r().status==="pending")}function s(r,o,l){const d=Ts(e,r,o,(p,u)=>{a(),l&&l(p,u)});return i.push(d),d}function n(r){return i.find(o=>r(o))||null}return{query:s,find:n,setIndex:r=>{e.index=r},getIndex:()=>e.index,cleanup:a}}function It(){}const ze=Object.create(null);function Cs(t){if(!ze[t]){const e=Re(t);if(!e)return;ze[t]={config:e,redundancy:gi(e)}}return ze[t]}function mi(t,e,i){let a,s;if(typeof t=="string"){const n=We(t);if(!n)return i(void 0,424),It;s=n.send;const r=Cs(t);r&&(a=r.redundancy)}else{const n=dt(t);if(n){a=gi(n);const r=We(t.resources?t.resources[0]:"");r&&(s=r.send)}}return!a||!s?(i(void 0,424),It):a.query(e,s,i)().abort}function Ot(){}function Es(t){t.iconsLoaderFlag||(t.iconsLoaderFlag=!0,setTimeout(()=>{t.iconsLoaderFlag=!1,xs(t)}))}function Ps(t){const e=[],i=[];return t.forEach(a=>{(a.match(oi)?e:i).push(a)}),{valid:e,invalid:i}}function ue(t,e,i){function a(){const s=t.pendingIcons;e.forEach(n=>{s&&s.delete(n),t.icons[n]||t.missing.add(n)})}if(i&&typeof i=="object")try{if(!di(t,i).length){a();return}}catch(s){console.error(s)}a(),Es(t)}function jt(t,e){t instanceof Promise?t.then(i=>{e(i)}).catch(()=>{e(null)}):e(t)}function Ds(t,e){t.iconsToLoad?t.iconsToLoad=t.iconsToLoad.concat(e).sort():t.iconsToLoad=e,t.iconsQueueFlag||(t.iconsQueueFlag=!0,setTimeout(()=>{t.iconsQueueFlag=!1;const{provider:i,prefix:a}=t,s=t.iconsToLoad;if(delete t.iconsToLoad,!s||!s.length)return;const n=t.loadIcon;if(t.loadIcons&&(s.length>1||!n)){jt(t.loadIcons(s,a,i),d=>{ue(t,s,d)});return}if(n){s.forEach(d=>{jt(n(d,a,i),p=>{ue(t,[d],p?{prefix:a,icons:{[d]:p}}:null)})});return}const{valid:r,invalid:o}=Ps(s);if(o.length&&ue(t,o,null),!r.length)return;const l=a.match(oi)?We(i):null;if(!l){ue(t,r,null);return}l.prepare(i,a,r).forEach(d=>{mi(i,d,p=>{ue(t,d.icons,p)})})}))}const ut=(t,e)=>{const i=ks(_s(t,!0,ui()));if(!i.pending.length){let o=!0;return e&&setTimeout(()=>{o&&e(i.loaded,i.missing,i.pending,Ot)}),()=>{o=!1}}const a=Object.create(null),s=[];let n,r;return i.pending.forEach(o=>{const{provider:l,prefix:d}=o;if(d===r&&l===n)return;n=l,r=d,s.push(U(l,d));const p=a[l]||(a[l]=Object.create(null));p[d]||(p[d]=[])}),i.pending.forEach(o=>{const{provider:l,prefix:d,name:p}=o,u=U(l,d),m=u.pendingIcons||(u.pendingIcons=new Set);m.has(p)||(m.add(p),a[l][d].push(p))}),s.forEach(o=>{const l=a[o.provider][o.prefix];l.length&&Ds(o,l)}),e?ws(e,i,s):Ot},Is=t=>new Promise((e,i)=>{const a=typeof t=="string"?$e(t,!0):t;if(!a){i(t);return}ut([a||t],s=>{if(s.length&&a){const n=ye(a);if(n){e({...xe,...n});return}}i(t)})});function Lt(t){try{const e=typeof t=="string"?JSON.parse(t):t;if(typeof e.body=="string")return{...e}}catch{}}function Os(t,e){if(typeof t=="object")return{data:Lt(t),value:t};if(typeof t!="string")return{value:t};if(t.includes("{")){const n=Lt(t);if(n)return{data:n,value:t}}const i=$e(t,!0,!0);if(!i)return{value:t};const a=ye(i);if(a!==void 0||!i.prefix)return{value:t,name:i,data:a};const s=ut([i],()=>e(t,i,ye(i)));return{value:t,name:i,loading:s}}let fi=!1;try{fi=navigator.vendor.indexOf("Apple")===0}catch{}function js(t,e){switch(e){case"svg":case"bg":case"mask":return e}return e!=="style"&&(fi||t.indexOf("<a")===-1)?"svg":t.indexOf("currentColor")===-1?"bg":"mask"}const Ls=/(-?[0-9.]*[0-9]+[0-9.]*)/g,Ms=/^-?[0-9.]*[0-9]+[0-9.]*$/g;function Qe(t,e,i){if(e===1)return t;if(i=i||100,typeof t=="number")return Math.ceil(t*e*i)/i;if(typeof t!="string")return t;const a=t.split(Ls);if(a===null||!a.length)return t;const s=[];let n=a.shift(),r=Ms.test(n);for(;;){if(r){const o=parseFloat(n);isNaN(o)?s.push(n):s.push(Math.ceil(o*e*i)/i)}else s.push(n);if(n=a.shift(),n===void 0)return s.join("");r=!r}}function Ns(t,e="defs"){let i="";const a=t.indexOf("<"+e);for(;a>=0;){const s=t.indexOf(">",a),n=t.indexOf("</"+e);if(s===-1||n===-1)break;const r=t.indexOf(">",n);if(r===-1)break;i+=t.slice(s+1,n).trim(),t=t.slice(0,a).trim()+t.slice(r+1)}return{defs:i,content:t}}function Rs(t,e){return t?"<defs>"+t+"</defs>"+e:e}function Us(t,e,i){const a=Ns(t);return Rs(a.defs,e+a.content+i)}const qs=t=>t==="unset"||t==="undefined"||t==="none";function bi(t,e){const i={...xe,...t},a={...ri,...e},s={left:i.left,top:i.top,width:i.width,height:i.height};let n=i.body;[i,a].forEach(k=>{const _=[],I=k.hFlip,N=k.vFlip;let O=k.rotate;I?N?O+=2:(_.push("translate("+(s.width+s.left).toString()+" "+(0-s.top).toString()+")"),_.push("scale(-1 1)"),s.top=s.left=0):N&&(_.push("translate("+(0-s.left).toString()+" "+(s.height+s.top).toString()+")"),_.push("scale(1 -1)"),s.top=s.left=0);let E;switch(O<0&&(O-=Math.floor(O/4)*4),O=O%4,O){case 1:E=s.height/2+s.top,_.unshift("rotate(90 "+E.toString()+" "+E.toString()+")");break;case 2:_.unshift("rotate(180 "+(s.width/2+s.left).toString()+" "+(s.height/2+s.top).toString()+")");break;case 3:E=s.width/2+s.left,_.unshift("rotate(-90 "+E.toString()+" "+E.toString()+")");break}O%2===1&&(s.left!==s.top&&(E=s.left,s.left=s.top,s.top=E),s.width!==s.height&&(E=s.width,s.width=s.height,s.height=E)),_.length&&(n=Us(n,'<g transform="'+_.join(" ")+'">',"</g>"))});const r=a.width,o=a.height,l=s.width,d=s.height;let p,u;r===null?(u=o===null?"1em":o==="auto"?d:o,p=Qe(u,l/d)):(p=r==="auto"?l:r,u=o===null?Qe(p,d/l):o==="auto"?d:o);const m={},b=(k,_)=>{qs(_)||(m[k]=_.toString())};b("width",p),b("height",u);const $=[s.left,s.top,l,d];return m.viewBox=$.join(" "),{attributes:m,viewBox:$,body:n}}function pt(t,e){let i=t.indexOf("xlink:")===-1?"":' xmlns:xlink="http://www.w3.org/1999/xlink"';for(const a in e)i+=" "+a+'="'+e[a]+'"';return'<svg xmlns="http://www.w3.org/2000/svg"'+i+">"+t+"</svg>"}function Fs(t){return t.replace(/"/g,"'").replace(/%/g,"%25").replace(/#/g,"%23").replace(/</g,"%3C").replace(/>/g,"%3E").replace(/\s+/g," ")}function zs(t){return"data:image/svg+xml,"+Fs(t)}function vi(t){return'url("'+zs(t)+'")'}const Hs=()=>{let t;try{if(t=fetch,typeof t=="function")return t}catch{}};let je=Hs();function Bs(t){je=t}function Vs(){return je}function Js(t,e){const i=Re(t);if(!i)return 0;let a;if(!i.maxURL)a=0;else{let s=0;i.resources.forEach(r=>{s=Math.max(s,r.length)});const n=e+".json?icons=";a=i.maxURL-s-i.path.length-n.length}return a}function Gs(t){return t===404}const Ks=(t,e,i)=>{const a=[],s=Js(t,e),n="icons";let r={type:n,provider:t,prefix:e,icons:[]},o=0;return i.forEach((l,d)=>{o+=l.length+1,o>=s&&d>0&&(a.push(r),r={type:n,provider:t,prefix:e,icons:[]},o=l.length),r.icons.push(l)}),a.push(r),a};function Ws(t){if(typeof t=="string"){const e=Re(t);if(e)return e.path}return"/"}const Qs=(t,e,i)=>{if(!je){i("abort",424);return}let a=Ws(e.provider);switch(e.type){case"icons":{const n=e.prefix,r=e.icons.join(","),o=new URLSearchParams({icons:r});a+=n+".json?"+o.toString();break}case"custom":{const n=e.uri;a+=n.slice(0,1)==="/"?n.slice(1):n;break}default:i("abort",400);return}let s=503;je(t+a).then(n=>{const r=n.status;if(r!==200){setTimeout(()=>{i(Gs(r)?"abort":"next",r)});return}return s=501,n.json()}).then(n=>{if(typeof n!="object"||n===null){setTimeout(()=>{n===404?i("abort",n):i("next",s)});return}setTimeout(()=>{i("success",n)})}).catch(()=>{i("next",s)})},Ys={prepare:Ks,send:Qs};function Zs(t,e,i){U(i||"",e).loadIcons=t}function Xs(t,e,i){U(i||"",e).loadIcon=t}const He="data-style";let yi="";function ea(t){yi=t}function Mt(t,e){let i=Array.from(t.childNodes).find(a=>a.hasAttribute&&a.hasAttribute(He));i||(i=document.createElement("style"),i.setAttribute(He,He),t.appendChild(i)),i.textContent=":host{display:inline-block;vertical-align:"+(e?"-0.125em":"0")+"}span,svg{display:block;margin:auto}"+yi}function xi(){Pt("",Ys),ui(!0);let t;try{t=window}catch{}if(t){if(t.IconifyPreload!==void 0){const i=t.IconifyPreload,a="Invalid IconifyPreload syntax.";typeof i=="object"&&i!==null&&(i instanceof Array?i:[i]).forEach(s=>{try{(typeof s!="object"||s===null||s instanceof Array||typeof s.icons!="object"||typeof s.prefix!="string"||!Et(s))&&console.error(a)}catch{console.error(a)}})}if(t.IconifyProviders!==void 0){const i=t.IconifyProviders;if(typeof i=="object"&&i!==null)for(const a in i){const s="IconifyProviders["+a+"] is invalid.";try{const n=i[a];if(typeof n!="object"||!n||n.resources===void 0)continue;Dt(a,n)||console.error(s)}catch{console.error(s)}}}}return{iconLoaded:vs,getIcon:ys,listIcons:bs,addIcon:pi,addCollection:Et,calculateSize:Qe,buildIcon:bi,iconToHTML:pt,svgToURL:vi,loadIcons:ut,loadIcon:Is,addAPIProvider:Dt,setCustomIconLoader:Xs,setCustomIconsLoader:Zs,appendCustomStyle:ea,_api:{getAPIConfig:Re,setAPIModule:Pt,sendAPIQuery:mi,setFetch:Bs,getFetch:Vs,listAPIProviders:Ss}}}const Ye={"background-color":"currentColor"},$i={"background-color":"transparent"},Nt={image:"var(--svg)",repeat:"no-repeat",size:"100% 100%"},Rt={"-webkit-mask":Ye,mask:Ye,background:$i};for(const t in Rt){const e=Rt[t];for(const i in Nt)e[t+"-"+i]=Nt[i]}function Ut(t){return t?t+(t.match(/^[-0-9.]+$/)?"px":""):"inherit"}function ta(t,e,i){const a=document.createElement("span");let s=t.body;s.indexOf("<a")!==-1&&(s+="<!-- "+Date.now()+" -->");const n=t.attributes,r=pt(s,{...n,width:e.width+"",height:e.height+""}),o=vi(r),l=a.style,d={"--svg":o,width:Ut(n.width),height:Ut(n.height),...i?Ye:$i};for(const p in d)l.setProperty(p,d[p]);return a}let ge;function ia(){try{ge=window.trustedTypes.createPolicy("iconify",{createHTML:t=>t})}catch{ge=null}}function sa(t){return ge===void 0&&ia(),ge?ge.createHTML(t):t}function aa(t){const e=document.createElement("span"),i=t.attributes;let a="";i.width||(a="width: inherit;"),i.height||(a+="height: inherit;"),a&&(i.style=a);const s=pt(t.body,i);return e.innerHTML=sa(s),e.firstChild}function Ze(t){return Array.from(t.childNodes).find(e=>{const i=e.tagName&&e.tagName.toUpperCase();return i==="SPAN"||i==="SVG"})}function qt(t,e){const i=e.icon.data,a=e.customisations,s=bi(i,a);a.preserveAspectRatio&&(s.attributes.preserveAspectRatio=a.preserveAspectRatio);const n=e.renderedMode;let r;n==="svg"?r=aa(s):r=ta(s,{...xe,...i},n==="mask");const o=Ze(t);o?r.tagName==="SPAN"&&o.tagName===r.tagName?o.setAttribute("style",r.getAttribute("style")):t.replaceChild(r,o):t.appendChild(r)}function Ft(t,e,i){const a=i&&(i.rendered?i:i.lastRender);return{rendered:!1,inline:e,icon:t,lastRender:a}}function ra(t="iconify-icon"){let e,i;try{e=window.customElements,i=window.HTMLElement}catch{return}if(!e||!i)return;const a=e.get(t);if(a)return a;const s=["icon","mode","inline","noobserver","width","height","rotate","flip"],n=class extends i{_shadowRoot;_initialised=!1;_state;_checkQueued=!1;_connected=!1;_observer=null;_visible=!0;constructor(){super();const o=this._shadowRoot=this.attachShadow({mode:"open"}),l=this.hasAttribute("inline");Mt(o,l),this._state=Ft({value:""},l),this._queueCheck()}connectedCallback(){this._connected=!0,this.startObserver()}disconnectedCallback(){this._connected=!1,this.stopObserver()}static get observedAttributes(){return s.slice(0)}attributeChangedCallback(o){switch(o){case"inline":{const l=this.hasAttribute("inline"),d=this._state;l!==d.inline&&(d.inline=l,Mt(this._shadowRoot,l));break}case"noobserver":{this.hasAttribute("noobserver")?this.startObserver():this.stopObserver();break}default:this._queueCheck()}}get icon(){const o=this.getAttribute("icon");if(o&&o.slice(0,1)==="{")try{return JSON.parse(o)}catch{}return o}set icon(o){typeof o=="object"&&(o=JSON.stringify(o)),this.setAttribute("icon",o)}get inline(){return this.hasAttribute("inline")}set inline(o){o?this.setAttribute("inline","true"):this.removeAttribute("inline")}get observer(){return this.hasAttribute("observer")}set observer(o){o?this.setAttribute("observer","true"):this.removeAttribute("observer")}restartAnimation(){const o=this._state;if(o.rendered){const l=this._shadowRoot;if(o.renderedMode==="svg")try{l.lastChild.setCurrentTime(0);return}catch{}qt(l,o)}}get status(){const o=this._state;return o.rendered?"rendered":o.icon.data===null?"failed":"loading"}_queueCheck(){this._checkQueued||(this._checkQueued=!0,setTimeout(()=>{this._check()}))}_check(){if(!this._checkQueued)return;this._checkQueued=!1;const o=this._state,l=this.getAttribute("icon");if(l!==o.icon.value){this._iconChanged(l);return}if(!o.rendered||!this._visible)return;const d=this.getAttribute("mode"),p=Tt(this);(o.attrMode!==d||ds(o.customisations,p)||!Ze(this._shadowRoot))&&this._renderIcon(o.icon,p,d)}_iconChanged(o){const l=Os(o,(d,p,u)=>{const m=this._state;if(m.rendered||this.getAttribute("icon")!==d)return;const b={value:d,name:p,data:u};b.data?this._gotIconData(b):m.icon=b});l.data?this._gotIconData(l):this._state=Ft(l,this._state.inline,this._state)}_forceRender(){if(!this._visible){const o=Ze(this._shadowRoot);o&&this._shadowRoot.removeChild(o);return}this._queueCheck()}_gotIconData(o){this._checkQueued=!1,this._renderIcon(o,Tt(this),this.getAttribute("mode"))}_renderIcon(o,l,d){const p=js(o.data.body,d),u=this._state.inline;qt(this._shadowRoot,this._state={rendered:!0,icon:o,inline:u,customisations:l,attrMode:d,renderedMode:p})}startObserver(){if(!this._observer&&!this.hasAttribute("noobserver"))try{this._observer=new IntersectionObserver(o=>{const l=o.some(d=>d.isIntersecting);l!==this._visible&&(this._visible=l,this._forceRender())}),this._observer.observe(this)}catch{if(this._observer){try{this._observer.disconnect()}catch{}this._observer=null}}}stopObserver(){this._observer&&(this._observer.disconnect(),this._observer=null,this._visible=!0,this._connected&&this._forceRender())}};s.forEach(o=>{o in n.prototype||Object.defineProperty(n.prototype,o,{get:function(){return this.getAttribute(o)},set:function(l){l!==null?this.setAttribute(o,l):this.removeAttribute(o)}})});const r=xi();for(const o in r)n[o]=n.prototype[o]=r[o];return e.define(t,n),n}const na=ra()||xi(),{iconLoaded:cr,getIcon:dr,listIcons:ur,addIcon:pr,addCollection:hr,calculateSize:gr,buildIcon:mr,iconToHTML:fr,svgToURL:br,loadIcons:vr,loadIcon:yr,setCustomIconLoader:xr,setCustomIconsLoader:$r,addAPIProvider:wr,_api:kr}=na;class B extends Error{constructor(e,i){super(i),this.status=e,this.name="ApiRequestError"}}const Xe="upgrid-session-expired";let ke;async function wi(t){if(!t.ok){const e=await t.json().catch(()=>({error:t.statusText}));throw new B(t.status,e.error||t.statusText)}return t.status===204?void 0:t.json()}function oa(){return ke||(ke=fetch("/api/v1/auth/session").then(t=>wi(t)).then(()=>{}).finally(()=>{ke=void 0})),ke}function zt(){return window.dispatchEvent(new Event(Xe)),new B(401,"")}async function f(t,e){const i=()=>fetch(t,{...e,headers:{...e?.body?{"content-type":"application/json"}:{},...e?.headers}});let a=await i();if(a.status===401&&!t.startsWith("/api/v1/auth/")){await a.body?.cancel();try{await oa()}catch(s){throw s instanceof B&&s.status===401?zt():s}if(a=await i(),a.status===401)throw await a.body?.cancel(),zt()}return wi(a)}const la={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4m0-4h.01"/></g>'};var ca=Object.defineProperty,da=Object.getOwnPropertyDescriptor,se=(t,e,i,a)=>{for(var s=a>1?void 0:a?da(e,i):e,n=t.length-1,r;n>=0;n--)(r=t[n])&&(s=(a?r(e,i,s):r(s))||s);return a&&s&&ca(e,i,s),s};let q=class extends L{constructor(){super(...arguments),this.disabled=!1,this.focusable=!1,this.contained=!1,this.placement="top",this.label="",this.message=""}updated(){this.tabIndex=this.focusable&&!this.disabled?0:-1,this.label&&!this.disabled?(this.setAttribute("aria-label","Why this action is disabled"),this.setAttribute("aria-description",this.label)):(this.removeAttribute("aria-label"),this.removeAttribute("aria-description"))}render(){return c`<slot name="trigger"></slot><span class="popup" role="tooltip">${this.message||c`<slot></slot>`}</span>`}};q.styles=F`
    :host {
      position: relative;
      display: inline-flex;
      align-items: center;
    }
    :host([contained]) { position: static; }


    .popup {
      position: absolute;
      right: 0;
      z-index: 30;
      width: min(280px, calc(100dvw - 72px));
      border: 1px solid var(--line);
      border-radius: 9px;
      background: var(--panel-2);
      color: var(--text);
      box-shadow: 0 10px 30px var(--dialog-shadow);
      padding: 9px 10px;
      font-size: 12px;
      font-weight: 400;
      line-height: 1.45;
      opacity: 0;
      visibility: hidden;
      pointer-events: none;
      transition: opacity 140ms ease, visibility 140ms;
    }
    :host([contained]) .popup { width: min(280px, 100%); }

    :host([placement="top"]) .popup { bottom: calc(100% + 6px); }
    :host([placement="bottom"]) .popup { top: calc(100% + 6px); }
    :host(:not([disabled]):hover) .popup,
    :host(:not([disabled]):focus-within) .popup {
      opacity: 1;
      visibility: visible;
      pointer-events: auto;
    }

    @media (prefers-reduced-motion: reduce) {
      .popup { transition-duration: 0s; }
    }
  `;se([S({type:Boolean,reflect:!0})],q.prototype,"disabled",2);se([S({type:Boolean})],q.prototype,"focusable",2);se([S({type:Boolean,reflect:!0})],q.prototype,"contained",2);se([S({reflect:!0})],q.prototype,"placement",2);se([S()],q.prototype,"label",2);se([S()],q.prototype,"message",2);q=se([ie("upgrid-tooltip")],q);const ht=F`
  .form-field { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
  .title-with-help { position: relative; display: flex; align-items: center; gap: 3px; }
  .help-tooltip-trigger { display: grid; width: 28px; height: 28px; place-items: center; border: 0; border-radius: 7px; background: transparent; color: var(--muted); padding: 0; cursor: pointer; user-select: none; transition: background-color 160ms ease, color 160ms ease; }
  .help-tooltip-trigger:hover { background: var(--panel-2); color: var(--text); }
  .help-tooltip-trigger iconify-icon { width: 16px; height: 16px; font-size: 16px; }
  .help-tooltip-content a { display: inline-block; margin-top: 5px; color: var(--green); font-weight: 600; }
`;function Y(t,e,i,a){return c`
    <upgrid-tooltip placement="bottom" contained>
      <button slot="trigger" class="help-tooltip-trigger" type="button" aria-label=${e} aria-describedby=${t}>
        <iconify-icon .icon=${la} aria-hidden="true"></iconify-icon>
      </button>
      <span class="help-tooltip-content" id=${t}>
        ${i}
        ${a?c`<a href=${a.href} target="_blank" rel="noreferrer">${a.label}</a>`:null}
      </span>
    </upgrid-tooltip>
  `}function ua(t){const e="labels"in t?t.labels:null;return t.getAttribute("aria-label")??e?.item(0)?.textContent?.trim()??void 0}function ki(t){const e=ua(t);return t.validity.valueMissing&&e?`Please fill out ${e.toLocaleLowerCase()}`:e?`${e}: ${t.validationMessage}`:t.validationMessage}var pa=Object.defineProperty,ha=Object.getOwnPropertyDescriptor,z=(t,e,i,a)=>{for(var s=a>1?void 0:a?ha(e,i):e,n=t.length-1,r;n>=0;n--)(r=t[n])&&(s=(a?r(e,i,s):r(s))||s);return a&&s&&pa(e,i,s),s};function ga(t){for(let e=0;e<t.elements.length;e+=1){const i=t.elements.item(e);if(i instanceof HTMLElement&&"validity"in i&&!i.validity.valid)return i}}function Ht(t){return JSON.stringify(Array.from(new FormData(t),([e,i])=>[e,typeof i=="string"?i:`${i.name}:${i.size}:${i.lastModified}`]))}let M=class extends L{constructor(){super(...arguments),this.busy=!1,this.blocked=!1,this.trackChanges=!1,this.error="",this.baselineKey="",this.blockedMessage="Form is unavailable",this.message="",this.baseline="",this.form=null,this.button=null,this.formChanged=()=>this.updateState(),this.formReset=()=>queueMicrotask(()=>this.captureBaseline())}firstUpdated(){this.form=this.closest("form"),this.button=this.querySelector('button[type="submit"]'),this.form?.addEventListener("input",this.formChanged),this.form?.addEventListener("change",this.formChanged),this.form?.addEventListener("reset",this.formReset),this.captureBaseline()}disconnectedCallback(){this.form?.removeEventListener("input",this.formChanged),this.form?.removeEventListener("change",this.formChanged),this.form?.removeEventListener("reset",this.formReset),super.disconnectedCallback()}updated(t){t.get("busy")===!0&&!this.busy&&!this.error?queueMicrotask(()=>this.captureBaseline()):t.has("baselineKey")&&t.get("baselineKey")!==void 0&&queueMicrotask(()=>this.captureBaseline()),this.updateState()}captureBaseline(){!this.form||!this.trackChanges||this.changed!==void 0||(this.baseline=Ht(this.form),this.updateState())}updateState(){if(!this.form||!this.button)return;const t=ga(this.form),e=!this.trackChanges||(this.changed??Ht(this.form)!==this.baseline);this.message=this.error.trim()||(this.blocked?this.blockedMessage:"")||(t?ki(t):"");const i=this.busy||this.message.length>0||!e;this.button.disabled=i,this.toggleAttribute("disabled",i)}render(){return c`
      <upgrid-tooltip .disabled=${!this.message} .focusable=${!!this.message} .label=${this.message} .message=${this.message}>
        <slot name="trigger" slot="trigger"></slot>
      </upgrid-tooltip>
    `}};M.styles=F`
    :host { display: inline-flex; }
    :host([disabled]) { cursor: not-allowed; }
    ::slotted(button[slot="trigger"]:disabled) { pointer-events: none; }
  `;z([S({type:Boolean})],M.prototype,"busy",2);z([S({type:Boolean})],M.prototype,"blocked",2);z([S({attribute:!1})],M.prototype,"changed",2);z([S({type:Boolean})],M.prototype,"trackChanges",2);z([S()],M.prototype,"error",2);z([S({attribute:"baseline-key"})],M.prototype,"baselineKey",2);z([S({attribute:"blocked-message"})],M.prototype,"blockedMessage",2);z([g()],M.prototype,"message",2);M=z([ie("upgrid-form-submit")],M);function P({label:t,busy:e=!1,className:i="button",blocked:a=!1,changed:s,error:n="",baselineKey:r="",blockedMessage:o="Form is unavailable",trackChanges:l=!1}){return c`
    <upgrid-form-submit
      .busy=${e}
      .blocked=${a}
      .changed=${s}
      .error=${n}
      .trackChanges=${l||s!==void 0}
      .baselineKey=${r}
      .blockedMessage=${o}
    >
      <button slot="trigger" class=${i} type="submit" aria-busy=${e?"true":"false"}>${t}</button>
    </upgrid-form-submit>
  `}function Bt(t,e,i=!1){if(e==="telegram"){const a=String(t.get("bot_token")??"");return{type:"telegram",name:t.get("name"),bot_token:i&&!a?void 0:a,chat_id:t.get("chat_id"),default:t.get("default")==="on"}}if(e==="smtp"){const a=String(t.get("username")??""),s=String(t.get("password")??"");return{type:"smtp",name:t.get("name"),host:t.get("host"),port:Number(t.get("port")),security:t.get("security"),username:a||void 0,password:s||void 0,from:t.get("from"),to:t.get("to"),default:t.get("default")==="on"}}return{type:"webhook",name:t.get("name"),url:t.get("url"),headers:i?void 0:{},default:t.get("default")==="on"}}function et(t,e=[],i=!0,a=String(t.get("kind")??"http"),s=[]){const n=String(t.get("url")),r=a==="http"?n:`${a}://${n.replace(/^[a-z][a-z0-9+.-]*:\/\//i,"")}`;return{name:String(t.get("name")),kind:a,url:r,method:String(t.get("method")??"GET"),accepted_statuses:[{start:200,end:299}],follow_redirects:!0,max_redirects:5,interval_seconds:Number(t.get("interval")),timeout_seconds:Number(t.get("timeout")),failure_threshold:Number(t.get("failures")),locations:Number(t.get("locations")??1),headers:{},body:null,assertions:s,skip_tls_verification:!1,tls_ca_secret_id:Be(t,"tls_ca_secret_id"),tls_client_certificate_secret_id:Be(t,"tls_client_certificate_secret_id"),tls_client_private_key_secret_id:Be(t,"tls_client_private_key_secret_id"),notification_channel_ids:e,use_default_channels:i}}function Be(t,e){return String(t.get(e)??"")||null}var ma=Object.defineProperty,fa=Object.getOwnPropertyDescriptor,j=(t,e,i,a)=>{for(var s=a>1?void 0:a?fa(e,i):e,n=t.length-1,r;n>=0;n--)(r=t[n])&&(s=(a?r(e,i,s):r(s))||s);return a&&s&&ma(e,i,s),s};let D=class extends L{constructor(){super(...arguments),this.defaultChannel=!1,this.submitLabel="Create channel",this.cancelLabel="Cancel",this.disabled=!1,this.kind="webhook",this.isDefault=!1,this.saving=!1,this.testing=!1,this.message="",this.messageIsError=!1}willUpdate(t){t.has("channel")&&(this.kind=this.channel?.kind??"webhook",this.message="",this.messageIsError=!1),(t.has("channel")||t.has("defaultChannel"))&&(this.isDefault=this.channel?.default??this.defaultChannel)}changeKind(t){this.kind=t.target.value,this.message="",this.messageIsError=!1}formChanged(){this.messageIsError&&(this.message="",this.messageIsError=!1)}async save(t){t.preventDefault();const e=t.currentTarget,i=this.channel!==void 0;this.saving=!0,this.message="";try{const a=await f(i?`/api/v1/channels/${this.channel?.id}`:"/api/v1/channels",{method:i?"PUT":"POST",body:JSON.stringify(Bt(new FormData(e),this.kind,i))});e.reset(),this.kind=this.channel?.kind??"webhook",this.dispatchEvent(new CustomEvent("channel-saved",{detail:a,bubbles:!0,composed:!0}))}catch(a){this.showFailure("Save failed",a)}finally{this.saving=!1}}cancel(){this.dispatchEvent(new CustomEvent("channel-cancel",{bubbles:!0,composed:!0}))}async testConnection(t){const e=t.currentTarget.form;if(!e||![...e.querySelectorAll("[data-test-required]")].every(r=>r.reportValidity()))return;const a=this.channel!==void 0,s=Bt(new FormData(e),this.kind,a),n=this.channel?{...s,channel_id:this.channel.id}:s;this.testing=!0,this.message="";try{await f("/api/v1/channels/test",{method:"POST",body:JSON.stringify(n)}),this.message="Test sent",this.messageIsError=!1}catch(r){this.showFailure("Test failed",r)}finally{this.testing=!1}}showFailure(t,e){this.message=`${t}: ${e instanceof Error?e.message:String(e)}`,this.messageIsError=!0}render(){const t=this.disabled||this.saving||this.testing;return c`<form @submit=${this.save} @input=${this.formChanged}>
      <label>Type<select name="type" .value=${this.kind} ?disabled=${this.channel!==void 0||t} @change=${this.changeKind}><option value="webhook">Webhook</option><option value="telegram">Telegram</option><option value="smtp">SMTP email</option></select></label>
      <label>Name<input name="name" placeholder="On-call" .value=${this.channel?.name??""} required /></label>
      ${this.renderFields()}
      <label class="switch"><span>Default channel</span><input class="switch-control" name="default" type="checkbox" role="switch" .checked=${this.isDefault} ?disabled=${t} @change=${e=>this.isDefault=e.target.checked} /></label>
      ${this.message?c`<p class=${`channel-test-message${this.messageIsError?" error":""}`} role="status">${this.message}</p>`:h}
      <div class="dialog-actions"><button class="button secondary" type="button" ?disabled=${t} @click=${this.cancel}>${this.cancelLabel}</button><button class="button secondary" type="button" aria-busy=${this.testing} ?disabled=${t} @click=${this.testConnection}>${this.testing?"Sending...":"Send test"}</button>${P({label:this.saving?"Saving...":this.submitLabel,busy:this.saving,blocked:this.disabled||this.testing,error:this.messageIsError?this.message:"",baselineKey:this.channel?.id??"new",blockedMessage:this.testing?"Channel test is in progress":"Channel form is unavailable",trackChanges:this.channel!==void 0})}</div>
    </form>`}renderFields(){return this.kind==="webhook"?c`<label>Webhook URL<input name="url" type="url" placeholder="https://hooks.example.com/upgrid" .value=${this.channel?.destination??""} data-test-required required /></label>`:this.kind==="telegram"?c`
        <label><span class="title-with-help">Bot token ${Y("telegram-token-help","About Telegram bot token storage",this.channel?"Get a replacement token from Telegram's @BotFather. Leave this blank to keep the automatically managed secret, or enter the replacement token.":"Get a bot token from Telegram's @BotFather. Creating the channel encrypts it as an automatically managed secret. Test sends use the entered value without storing it.")}</span><input name="bot_token" type="password" autocomplete="off" placeholder=${this.channel?"Leave blank to keep current token":""} data-test-required ?required=${this.channel===void 0} /></label>
        <label>Chat ID<input name="chat_id" .value=${this.channel?.destination??""} data-test-required required /></label>
      `:c`
      <label>SMTP host<input name="host" placeholder="smtp.example.com" .value=${this.channel?.destination??""} data-test-required required /></label>
      <div class="row">
        <label>Port<input name="port" type="number" min="1" max="65535" .value=${String(this.channel?.port??587)} data-test-required required /></label>
        <label>Security<select name="security" .value=${this.channel?.security??"start_tls"}><option value="start_tls">STARTTLS</option><option value="tls">Implicit TLS</option><option value="none">Plaintext</option></select></label>
      </div>
      <label>Username<input name="username" autocomplete="username" .value=${this.channel?.username??""} /></label>
      <div class="form-field"><div class="title-with-help"><label for="smtp-password">Password</label>${Y("smtp-password-help","About SMTP password storage",this.channel?"Leave this blank to keep the automatically managed secret. Clear the username to disable authentication.":"Enter a username and password together to enable authentication. The password is encrypted as an automatically managed secret.")}</div><input id="smtp-password" name="password" type="password" autocomplete="off" placeholder=${this.channel?"Leave blank to keep current password":"Optional"} /></div>
      <label>From<input name="from" placeholder="UpGrid <upgrid@example.com>" .value=${this.channel?.from??""} data-test-required required /></label>
      <label>Recipient<input name="to" placeholder="on-call@example.com" .value=${this.channel?.to??""} data-test-required required /></label>
    `}};D.styles=F`
    ${ht}
    :host { display: block; }
    *, *::before, *::after { box-sizing: border-box; }
    form { display: grid; gap: 13px; padding: 20px 22px 22px; }
    label { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
    input, select { width: 100%; min-height: 44px; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; font: inherit; font-size: 16px; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease; }
    input:focus, select:focus { border-color: var(--focus); }
    button:focus-visible, input:focus-visible, select:focus-visible { outline: 2px solid var(--green); outline-offset: 2px; }
    button, input[type="checkbox"], select, .switch { cursor: pointer; user-select: none; }
    button.button:disabled { border-color: var(--disabled-border); background: var(--disabled-bg); color: var(--disabled-text); cursor: not-allowed; opacity: 1; }
    input:disabled, select:disabled { cursor: not-allowed; opacity: .65; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    .button { min-height: 44px; border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; white-space: nowrap; cursor: pointer; font: inherit; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    .button:hover { border-color: var(--button-hover-border); }
    .button:active { transform: translateY(1px); }
    .secondary { border-color: var(--line); background: transparent; color: var(--muted); }
    .switch { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .switch-control { width: 42px; min-height: 24px; height: 24px; flex: none; appearance: none; border-radius: 999px; background: var(--input-bg); padding: 2px; }
    .switch-control::after { display: block; width: 16px; height: 16px; border-radius: 50%; background: var(--muted); content: ""; transition: background-color 160ms ease, transform 160ms ease; }
    .switch-control:checked { border-color: var(--button-border); background: var(--button-bg); }
    .switch-control:checked::after { background: var(--button-text); transform: translateX(18px); }
    .form-field { display: grid; gap: 6px; }
    .title-with-help { display: flex; align-items: center; gap: 6px; color: var(--muted); font-size: 14px; }
    .channel-test-message { margin: 5px 0 0; border: 1px solid var(--line); border-radius: 9px; background: var(--panel-2); color: var(--green); padding: 10px 12px; overflow-wrap: anywhere; white-space: normal; }
    .channel-test-message.error { border-color: var(--notice-border); background: var(--notice-bg); color: var(--notice-text); }
    .dialog-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 5px; }
    @media (max-width: 620px) { .row { grid-template-columns: 1fr; } .dialog-actions { flex-wrap: wrap; } }
    @media (prefers-reduced-motion: reduce) { input, select, .button, .switch-control, .switch-control::after { transition-duration: 0s; } }
  `;j([S({attribute:!1})],D.prototype,"channel",2);j([S({type:Boolean,attribute:"default-channel"})],D.prototype,"defaultChannel",2);j([S({attribute:"submit-label"})],D.prototype,"submitLabel",2);j([S({attribute:"cancel-label"})],D.prototype,"cancelLabel",2);j([S({type:Boolean})],D.prototype,"disabled",2);j([g()],D.prototype,"kind",2);j([g()],D.prototype,"isDefault",2);j([g()],D.prototype,"saving",2);j([g()],D.prototype,"testing",2);j([g()],D.prototype,"message",2);j([g()],D.prototype,"messageIsError",2);D=j([ie("notification-channel-form")],D);const ba={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><path d="M22 12h-6l-2 3h-4l-2-3H2"/><path d="M5.45 5.11L2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z"/></g>'};var va=Object.getOwnPropertyDescriptor,ya=(t,e,i,a)=>{for(var s=a>1?void 0:a?va(e,i):e,n=t.length-1,r;n>=0;n--)(r=t[n])&&(s=r(s)||s);return s};let tt=class extends L{render(){return c`<div class="state"><span class="illustration" aria-hidden="true"><iconify-icon .icon=${ba}></iconify-icon></span><p><slot></slot></p></div>`}};tt.styles=F`
    :host {
      display: block;
      margin: 14px 0;
    }

    .state {
      box-sizing: border-box;
      display: grid;
      min-height: 132px;
      place-content: center;
      justify-items: center;
      gap: 11px;
      padding: 22px 18px;
      color: var(--muted);
      text-align: center;
    }

    .illustration {
      display: grid;
      place-items: center;
      color: var(--green);
    }

    iconify-icon {
      width: 23px;
      height: 23px;
      font-size: 23px;
    }

    p {
      max-width: 34ch;
      margin: 0;
      font-size: 13px;
      line-height: 1.45;
    }
  `;tt=ya([ie("upgrid-empty-state")],tt);const xa={CHILD:2},$a=t=>(...e)=>({_$litDirective$:t,values:e});let wa=class{constructor(e){}get _$AU(){return this._$AM._$AU}_$AT(e,i,a){this._$Ct=e,this._$AM=i,this._$Ci=a}_$AS(e,i){return this.update(e,i)}update(e,i){return this.render(...i)}};const{I:ka}=Xi,Vt=t=>t,Jt=()=>document.createComment(""),pe=(t,e,i)=>{const a=t._$AA.parentNode,s=e===void 0?t._$AB:e._$AA;if(i===void 0){const n=a.insertBefore(Jt(),s),r=a.insertBefore(Jt(),s);i=new ka(n,r,t,t.options)}else{const n=i._$AB.nextSibling,r=i._$AM,o=r!==t;if(o){let l;i._$AQ?.(t),i._$AM=t,i._$AP!==void 0&&(l=t._$AU)!==r._$AU&&i._$AP(l)}if(n!==s||o){let l=i._$AA;for(;l!==n;){const d=Vt(l).nextSibling;Vt(a).insertBefore(l,s),l=d}}}return i},K=(t,e,i=t)=>(t._$AI(e,i),t),_a={},Sa=(t,e=_a)=>t._$AH=e,Aa=t=>t._$AH,Ve=t=>{t._$AR(),t._$AA.remove()};const Gt=(t,e,i)=>{const a=new Map;for(let s=e;s<=i;s++)a.set(t[s],s);return a},Ta=$a(class extends wa{constructor(t){if(super(t),t.type!==xa.CHILD)throw Error("repeat() can only be used in text expressions")}dt(t,e,i){let a;i===void 0?i=e:e!==void 0&&(a=e);const s=[],n=[];let r=0;for(const o of t)s[r]=a?a(o,r):r,n[r]=i(o,r),r++;return{values:n,keys:s}}render(t,e,i){return this.dt(t,e,i).values}update(t,[e,i,a]){const s=Aa(t),{values:n,keys:r}=this.dt(e,i,a);if(!Array.isArray(s))return this.ut=r,n;const o=this.ut??=[],l=[];let d,p,u=0,m=s.length-1,b=0,$=n.length-1;for(;u<=m&&b<=$;)if(s[u]===null)u++;else if(s[m]===null)m--;else if(o[u]===r[b])l[b]=K(s[u],n[b]),u++,b++;else if(o[m]===r[$])l[$]=K(s[m],n[$]),m--,$--;else if(o[u]===r[$])l[$]=K(s[u],n[$]),pe(t,l[$+1],s[u]),u++,$--;else if(o[m]===r[b])l[b]=K(s[m],n[b]),pe(t,s[u],s[m]),m--,b++;else if(d===void 0&&(d=Gt(r,b,$),p=Gt(o,u,m)),d.has(o[u]))if(d.has(o[m])){const k=p.get(r[b]),_=k!==void 0?s[k]:null;if(_===null){const I=pe(t,s[u]);K(I,n[b]),l[b]=I}else l[b]=K(_,n[b]),pe(t,s[u],_),s[k]=null;b++}else Ve(s[m]),m--;else Ve(s[u]),u++;for(;b<=$;){const k=pe(t,l[$+1]);K(k,n[b]),l[b++]=k}for(;u<=m;){const k=s[u++];k!==null&&Ve(k)}return this.ut=r,Sa(t,l),X}}),_i=F`
  .panel {
    overflow: hidden;
    border: 1px solid var(--line);
    border-radius: 16px;
    background: var(--panel-surface);
    box-shadow: 0 16px 48px var(--panel-shadow);
    transition: background-color 180ms ease, border-color 180ms ease, box-shadow 180ms ease;
  }
  .panel-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    border-bottom: 1px solid var(--line);
    padding: 17px 20px;
  }
  .panel-head h2 {
    margin: 0;
    font-size: 14px;
  }
  .card-meta {
    color: var(--muted);
    font-size: 12px;
    white-space: nowrap;
  }
  .card-actions,
  .card-footer {
    display: flex;
    align-items: center;
    justify-content: flex-end;
    gap: 12px;
  }
  .card-footer {
    border-top: 1px solid var(--line);
    padding: 14px 20px;
  }
  @media (prefers-reduced-motion: reduce) {
    .panel {
      transition-duration: 0s;
    }
  }
`;function Ca(t){const e=t.variant&&t.variant!=="primary"?`button ${t.variant}`:"button";return c`
    <button
      class=${e}
      type="button"
      ?disabled=${t.disabled}
      aria-busy=${t.busy?"true":h}
      aria-label=${t.ariaLabel??h}
      title=${t.title??h}
      @click=${t.onClick}
    >
      ${t.label}
    </button>
  `}function T({title:t,label:e,tooltip:i,metadata:a,actions:s=[],content:n,footer:r,className:o}){const l=e??t,d=o?`panel ${o}`:"panel";return c`
    <section class=${d} aria-label=${l??h}>
      ${t?c`
            <div class="panel-head">
              ${i?c`<div class="title-with-help"><h2>${t}</h2>${Y(i.id,i.label,i.message,i.link)}</div>`:c`<h2>${t}</h2>`}
              ${a!==void 0||s.length?c`<div class="card-actions">${a!==void 0?c`<span class="card-meta">${a}</span>`:h}${Ta(s,p=>p.key??p.ariaLabel??p.label,Ca)}</div>`:h}
            </div>
          `:h}
      ${n}
      ${r?c`<div class="card-footer">${r}</div>`:h}
    </section>
  `}var Ea=Object.defineProperty,Pa=Object.getOwnPropertyDescriptor,we=(t,e,i,a)=>{for(var s=a>1?void 0:a?Pa(e,i):e,n=t.length-1,r;n>=0;n--)(r=t[n])&&(s=(a?r(e,i,s):r(s))||s);return a&&s&&Ea(e,i,s),s};let ee=class extends L{constructor(){super(...arguments),this.channels=[],this.saving=!1,this.error=""}connectedCallback(){super.connectedCallback(),this.loadChannels()}updated(t){t.has("setup")&&this.loadChannels()}async loadChannels(){if(!(!this.setup?.cluster_ready||this.setup.phase!=="target"))try{this.channels=await f("/api/v1/channels")}catch(t){this.fail(t)}}submittedNodeName(){return this.shadowRoot?.querySelector("#setup-node-name")?.value.trim()??""}async createCluster(t){if(t.preventDefault(),!window.confirm("Create a new single-node cluster?"))return;const e=new FormData(t.currentTarget),i=String(e.get("admin_username")??"").trim(),a=String(e.get("admin_password")??"");await this.choose("/api/v1/setup/new-cluster",{node_name:this.submittedNodeName(),admin_username:i,admin_password:a},{username:i,password:a})}async joinCluster(t){t.preventDefault();const e=t.currentTarget,i=new FormData(e);await this.choose("/api/v1/cluster/join",{node_name:this.submittedNodeName(),join_link:String(i.get("join_link")??"").trim()})}async choose(t,e,i){this.saving=!0,this.error="";try{await f(t,{method:"POST",body:JSON.stringify(e)}),await this.waitForCluster(i)}catch(a){this.fail(a),this.saving=!1}}async waitForCluster(t){for(let e=0;e<120;e+=1){const{promise:i,resolve:a}=Promise.withResolvers();window.setTimeout(a,250),await i;try{t&&await f("/api/v1/auth/login",{method:"POST",body:JSON.stringify(t)});const s=await f("/api/v1/setup");if(s.cluster_ready){this.changed(s);return}}catch(s){if(!t&&s instanceof B&&s.status===401){window.location.assign("/");return}}}throw new Error("Cluster setup did not finish within 30 seconds")}async createTarget(t){t.preventDefault();const e=new FormData(t.currentTarget),i=et(e,e.getAll("channel_id").map(String));await this.createResource("/api/v1/targets",i)}async createResource(t,e){this.saving=!0;try{await f(t,{method:"POST",body:JSON.stringify(e)}),await this.next()}catch(i){this.fail(i),this.saving=!1}}async next(){this.saving=!0;try{this.changed(await f("/api/v1/setup/next",{method:"POST"}))}catch(t){this.fail(t),this.saving=!1}}changed(t){this.saving=!1,this.dispatchEvent(new CustomEvent("setup-changed",{detail:t,bubbles:!0,composed:!0}))}fail(t){this.error=t instanceof Error?t.message:String(t)}render(){return c`<section class="flow" aria-label="UpGrid setup" @input=${()=>this.error=""}>
      ${this.error?c`<div class="notice" role="alert">${this.error}</div>`:h}
      ${this.setup.phase==="cluster"?this.renderCluster():this.setup.phase==="channel"?this.renderChannel():this.renderTarget()}
    </section>`}renderCluster(){return c`
      <span class="eyebrow">First-run setup</span><h1>Choose your cluster</h1>
      <p class="lead">Review this node’s name, then create a new cluster or use an invitation to join one.</p>
      ${T({content:c`
          <div class="cluster-identity">
            <label for="setup-node-name">Node name<input id="setup-node-name" .value=${this.setup.node_name} required /></label>
          </div>
          <form class="cluster-create" @submit=${this.createCluster}>
            <div class="cluster-copy"><h2>Start a new cluster</h2><p>Create its first replicated administrator identity.</p></div>
            <div class="cluster-create-fields">
              <label>Administrator username<input name="admin_username" autocomplete="username" value="admin" required /></label>
              <label>Administrator password<input name="admin_password" type="password" minlength="12" autocomplete="new-password" required /></label>
            </div>
            ${P({label:this.saving?"Setting up...":"Create new cluster",busy:this.saving,error:this.error})}
          </form>
          <div class="cluster-divider"><span>Or</span></div>
          <form class="cluster-join" @submit=${this.joinCluster}>
            <div class="cluster-copy"><h2>Join an existing cluster</h2><p>Paste an <code>up://</code> join token from a current member.</p></div>
            <div class="cluster-join-fields">
              <label>Join token<input name="join_link" type="url" pattern="up://.*" placeholder="up://node.example/token" autocomplete="off" required /></label>
              ${P({label:"Join cluster",className:"secondary",busy:this.saving,error:this.error})}
            </div>
          </form>
        `})}`}renderChannel(){return c`
      <span class="eyebrow">Optional · step 2 of 3</span><h1>Add a notification channel</h1>
      <p class="lead">Send availability transitions through Telegram, SMTP, or a webhook. <span class="count">${this.setup.channel_count} already configured</span></p>
      ${T({content:c`<notification-channel-form default-channel submit-label="Create and continue" cancel-label="Skip" .disabled=${this.saving} @channel-cancel=${this.next} @channel-saved=${this.next}></notification-channel-form>`})}`}renderTarget(){return c`
      <span class="eyebrow">Optional · step 3 of 3</span><h1>Monitor your first target</h1>
      <p class="lead">Configure an HTTP endpoint now or continue to the dashboard. <span class="count">${this.setup.target_count} already configured</span></p>
      ${T({content:c`
          <form class="choice" @submit=${this.createTarget}>
            <label>Name<input name="name" placeholder="Production API" required /></label>
            <label>URL<input name="url" type="url" placeholder="https://example.com/health" required /></label>
            <div class="row"><label>Method<input name="method" value="GET" required /></label><label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label></div>
            <div class="row"><label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label><label>Failures before down<input name="failures" type="number" min="1" value="3" required /></label></div>
            ${this.channels.length?c`<fieldset><legend>Notification channels</legend>${this.channels.map(t=>c`<label class="switch"><span>${t.name}</span><input class="switch-control" name="channel_id" type="checkbox" role="switch" value=${t.id} /></label>`)}</fieldset>`:c`<upgrid-empty-state>No notification channels are available</upgrid-empty-state>`}
            <div class="actions"><button class="secondary" type="button" @click=${this.next} ?disabled=${this.saving}>Skip</button>${P({label:"Create and finish",busy:this.saving,error:this.error})}</div>
          </form>
        `})}`}};ee.styles=F`
    :host { display: block; }
    *, *::before, *::after { box-sizing: border-box; }
    .flow { width: min(760px, 100%); margin: 0 auto; }
    ${_i}
    .eyebrow { color: var(--muted); font-size: 12px; letter-spacing: .16em; text-transform: uppercase; }
    h1 { margin: 5px 0 8px; font-size: clamp(30px, 5vw, 46px); letter-spacing: -.04em; }
    .lead { margin: 0 0 16px; color: var(--muted); font-size: 15px; }
    .choice { display: grid; gap: 14px; padding: 22px; border-top: 1px solid var(--line); }
    .choice:first-child { border-top: 0; }
    .choice h2 { margin: 0; font-size: 17px; }
    .choice p { margin: -8px 0 0; color: var(--muted); }
    .cluster-identity, .cluster-create, .cluster-join { padding: 18px; }
    .cluster-identity { border-bottom: 1px solid var(--line); }
    .cluster-create { display: grid; gap: 14px; }
    .cluster-create-fields { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    .cluster-create button { justify-self: end; }
    .cluster-copy h2 { margin: 0; font-size: 17px; }
    .cluster-copy p { margin: 2px 0 0; color: var(--muted); }
    .cluster-divider { display: flex; align-items: center; gap: 12px; color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .12em; }
    .cluster-divider::before, .cluster-divider::after { height: 1px; flex: 1; background: var(--line); content: ""; }
    .cluster-join { display: grid; gap: 10px; }
    .cluster-join-fields { display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: end; gap: 10px; }
    .cluster-join-fields label { min-width: 0; }
    .cluster-join-fields button { height: 44px; white-space: nowrap; }
    form { display: grid; gap: 13px; }
    label { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
    .switch { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .switch-label { display: flex; min-width: 0; align-items: center; gap: 8px; }
    .switch-control { width: 42px; min-height: 24px; height: 24px; flex: none; appearance: none; border: 1px solid var(--line); border-radius: 999px; background: var(--input-bg); padding: 2px; cursor: pointer; }
    .switch-control::after { display: block; width: 16px; height: 16px; border-radius: 50%; background: var(--muted); content: ""; transition: background-color 160ms ease, transform 160ms ease; }
    .switch-control:checked { border-color: var(--button-border); background: var(--button-bg); }
    .switch-control:checked::after { background: var(--button-text); transform: translateX(18px); }
    fieldset { display: grid; gap: 8px; min-width: 0; margin: 0; border: 0; padding: 0; }
    legend { margin-bottom: 4px; padding: 0; color: var(--text); font-size: 14px; }
    input:not([type="checkbox"]), select { width: 100%; min-height: 44px; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; font: inherit; font-size: 16px; transition: border-color 160ms ease, opacity 160ms ease; }
    input:not([type="checkbox"]):focus, select:focus { border-color: var(--focus); }
    button:focus-visible, input:focus-visible, select:focus-visible { outline: 2px solid var(--green); outline-offset: 2px; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    .actions { display: flex; justify-content: flex-end; gap: 9px; margin-top: 5px; }
    button { display: inline-flex; min-height: 44px; align-items: center; justify-content: center; border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; cursor: pointer; font: inherit; transition: background-color 160ms ease, border-color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    button:hover { border-color: var(--button-hover-border); }
    button:active { transform: translateY(1px); }
    button:disabled { border-color: var(--disabled-border); background: var(--disabled-bg); color: var(--disabled-text); cursor: not-allowed; opacity: 1; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .notice { margin-bottom: 16px; border: 1px solid var(--notice-border); border-radius: 10px; background: var(--notice-bg); color: var(--notice-text); padding: 10px 12px; }
    .count { display: inline-block; margin-top: 6px; color: var(--green); font-size: 12px; }
    @media (max-width: 620px) { .row, .cluster-create-fields, .cluster-join-fields { grid-template-columns: 1fr; } .cluster-create button, .cluster-join button { justify-self: end; } }
    @media (max-height: 650px) and (min-width: 621px) {
      h1 { margin: 2px 0 4px; font-size: 30px; }
      .lead { margin-bottom: 8px; font-size: 13px; }
      .cluster-identity, .cluster-create, .cluster-join { padding: 8px 14px; }
      .cluster-create { grid-template-columns: minmax(0, 1fr) auto; gap: 8px; }
      .cluster-create .cluster-copy { grid-column: 1 / -1; }
      .cluster-create button { align-self: end; }
      .cluster-copy p { display: none; }
      .cluster-join { grid-template-columns: auto minmax(0, 1fr); align-items: end; }
      input:not([type="checkbox"]), button { min-height: 38px; }
      .cluster-join-fields button { height: 44px; }
    }
  `;we([S({attribute:!1})],ee.prototype,"setup",2);we([g()],ee.prototype,"channels",2);we([g()],ee.prototype,"saving",2);we([g()],ee.prototype,"error",2);ee=we([ie("upgrid-setup")],ee);const Da={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 5v14m7-7l-7 7l-7-7"/>'},Ia={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m5 12l7-7l7 7m-7 7V5"/>'};var Oa=Object.defineProperty,ja=Object.getOwnPropertyDescriptor,Ue=(t,e,i,a)=>{for(var s=a>1?void 0:a?ja(e,i):e,n=t.length-1,r;n>=0;n--)(r=t[n])&&(s=(a?r(e,i,s):r(s))||s);return a&&s&&Oa(e,i,s),s};const La={body_contains:"Body contains",body_regex:"Body regex",json_path:"JSONPath",response_header:"Response header",latency:"Latency threshold",script:"Script"};let te=class extends L{constructor(){super(...arguments),this.assertions=[],this.targetId="new",this.draft=[],this.loadedTarget="",this.internals=this.attachInternals()}get value(){return structuredClone(this.draft)}get validity(){return this.internals.validity}get validationMessage(){return this.internals.validationMessage}checkValidity(){return this.internals.checkValidity()}reportValidity(){return this.internals.reportValidity()}willUpdate(t){t.has("targetId")&&this.loadedTarget!==this.targetId&&(this.loadedTarget=this.targetId,this.draft=structuredClone(this.assertions))}updated(){this.internals.setFormValue(JSON.stringify(this.draft)),this.updateValidity()}formResetCallback(){this.draft=structuredClone(this.assertions),this.internals.setFormValue(JSON.stringify(this.draft))}add(){this.draft=[...this.draft,Kt("body_contains")],this.changed()}removeAssertion(t){this.draft=this.draft.filter((e,i)=>i!==t),this.changed()}move(t,e){const i=t+e;if(i<0||i>=this.draft.length)return;const a=[...this.draft];[a[t],a[i]]=[a[i],a[t]],this.draft=a,this.changed()}setKind(t,e){const i=e.currentTarget.value;this.replace(t,Kt(i))}set(t,e,i){const a=i.currentTarget,s={...this.draft[t],[e]:e==="max_ms"?Number(a.value):a.value||null};this.replace(t,s)}replace(t,e){this.draft=this.draft.map((i,a)=>a===t?e:i),this.changed()}changed(){this.internals.setFormValue(JSON.stringify(this.draft)),this.updateComplete.then(()=>{this.updateValidity(),this.dispatchEvent(new Event("input",{bubbles:!0,composed:!0}))})}updateValidity(){const t=this.renderRoot.querySelector("input:invalid, select:invalid, textarea:invalid");t?this.internals.setValidity({customError:!0},ki(t),t):this.internals.setValidity({})}render(){return c`
      <div class="assertions">
        <button class="add" type="button" aria-label="Add assertion" @click=${this.add}>Add assertion</button>
        <div class="assertion-list">
          ${this.draft.length?this.draft.map((t,e)=>this.renderAssertion(t,e)):c`<upgrid-empty-state>No assertions</upgrid-empty-state>`}
        </div>
      </div>
    `}renderAssertion(t,e){return c`
      <div class="assertion">
        <label>Type<select aria-label=${`Assertion ${e+1} type`} .value=${t.kind} @change=${i=>this.setKind(e,i)}>${Object.entries(La).map(([i,a])=>c`<option value=${i}>${a}</option>`)}</select></label>
        ${this.renderFields(t,e)}
        <div class="actions">
          <button class="icon-button move" type="button" aria-label=${`Move assertion ${e+1} up`} title="Move up" ?disabled=${e===0} @click=${()=>this.move(e,-1)}><iconify-icon .icon=${Ia} aria-hidden="true"></iconify-icon></button>
          <button class="icon-button move" type="button" aria-label=${`Move assertion ${e+1} down`} title="Move down" ?disabled=${e===this.draft.length-1} @click=${()=>this.move(e,1)}><iconify-icon .icon=${Da} aria-hidden="true"></iconify-icon></button>
          <button class="icon-button danger" type="button" aria-label=${`Remove assertion ${e+1}`} title="Remove assertion" @click=${()=>this.removeAssertion(e)}><iconify-icon .icon=${ne} aria-hidden="true"></iconify-icon></button>
        </div>
      </div>
    `}renderFields(t,e){switch(t.kind){case"body_contains":return c`<div class="fields single"><label>Required text<input aria-label=${`Assertion ${e+1} required text`} .value=${t.value} required @input=${i=>this.set(e,"value",i)} /></label></div>`;case"body_regex":return c`<div class="fields single"><label>Regular expression<input aria-label=${`Assertion ${e+1} regular expression`} .value=${t.pattern} required @input=${i=>this.set(e,"pattern",i)} /></label></div>`;case"json_path":return c`<div class="fields"><label>Path<input aria-label=${`Assertion ${e+1} JSONPath`} .value=${t.path} placeholder="$.status" required @input=${i=>this.set(e,"path",i)} /></label><label>Expected value (optional)<input aria-label=${`Assertion ${e+1} expected value`} .value=${t.expected??""} @input=${i=>this.set(e,"expected",i)} /></label></div>`;case"response_header":return c`<div class="fields"><label>Header name<input aria-label=${`Assertion ${e+1} header name`} .value=${t.name} placeholder="content-type" required @input=${i=>this.set(e,"name",i)} /></label><label>Exact value (optional)<input aria-label=${`Assertion ${e+1} header value`} .value=${t.value??""} @input=${i=>this.set(e,"value",i)} /></label></div>`;case"latency":return c`<div class="fields single"><label>Maximum milliseconds<input aria-label=${`Assertion ${e+1} maximum milliseconds`} type="number" min="1" .value=${String(t.max_ms)} required @input=${i=>this.set(e,"max_ms",i)} /></label></div>`;case"script":return c`<div class="fields single"><label><span class="title-with-help">Boolean Rhai expression ${Y(`script-assertion-${e+1}-help`,`About script assertion ${e+1}`,"Return true to pass. The script can read the response status, latency, body, final URL, and headers.",{href:"https://upgrid.rs/reference/script-assertions/",label:"Read the script assertion reference"})}</span><textarea aria-label=${`Assertion ${e+1} script`} required @input=${i=>this.set(e,"source",i)}>${t.source}</textarea></label></div>`;default:return h}}};te.formAssociated=!0;te.styles=F`
    ${ht}
    :host { display: grid; gap: 10px; }
    .assertions, .assertion-list { display: grid; gap: 10px; }
    .assertion-list { max-height: min(420px, 50vh); overflow-y: auto; padding-right: 4px; scrollbar-gutter: stable; }
    .assertion { display: grid; grid-template-columns: minmax(140px, 0.7fr) minmax(180px, 1.3fr) auto; gap: 8px; align-items: end; }
    .fields { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }
    .fields.single { grid-template-columns: 1fr; }
    label { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
    input, select, textarea { box-sizing: border-box; width: 100%; min-height: 44px; border: 1px solid var(--line); border-radius: 9px; background: var(--input-bg); color: var(--text); padding: 9px 10px; font-family: inherit; font-size: 16px; }
    textarea { min-height: 72px; resize: vertical; font-family: ui-monospace, monospace; }
    .actions { display: flex; align-items: flex-end; gap: 4px; }
    button { border: 1px solid var(--line); border-radius: 7px; background: var(--panel-2); color: var(--text); padding: 8px 10px; cursor: pointer; user-select: none; }
    button.icon-button:disabled, button.add:disabled { border-color: var(--disabled-border); background: var(--disabled-bg); color: var(--disabled-text); cursor: not-allowed; opacity: 1; }
    .icon-button { display: grid; width: 44px; height: 44px; min-height: 44px; place-items: center; border-radius: 9px; padding: 0; }
    .icon-button iconify-icon { display: inline-block; width: 16px; height: 16px; font-size: 16px; }
    .icon-button.move { color: var(--green); }
    .icon-button.danger { color: var(--danger-text); }
    .add { display: inline-flex; min-height: 34px; align-items: center; gap: 6px; justify-self: end; border-color: var(--line); background: var(--panel-2); color: var(--text); padding: 6px 10px; font-size: 13px; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, transform 120ms ease; }
    .add::before { color: var(--green); content: "+"; font-size: 18px; font-weight: 400; line-height: 12px; }
    .add:hover { border-color: var(--green); color: var(--green); }
    .add:active { transform: translateY(1px); }
    @media (max-width: 720px) { .assertion { grid-template-columns: 1fr; } .fields { grid-template-columns: 1fr; } }
  `;Ue([S({attribute:!1})],te.prototype,"assertions",2);Ue([S({attribute:"target-id"})],te.prototype,"targetId",2);Ue([g()],te.prototype,"draft",2);te=Ue([ie("http-assertion-editor")],te);function Kt(t){switch(t){case"body_contains":return{kind:t,value:""};case"body_regex":return{kind:t,pattern:""};case"json_path":return{kind:t,path:"$",expected:null};case"response_header":return{kind:t,name:"",value:null};case"latency":return{kind:t,max_ms:1e3};case"script":return{kind:t,source:"status == 200"}}}const Ma={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3a6 6 0 0 0 9 9a9 9 0 1 1-9-9Z"/>'},Na={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><rect width="20" height="14" x="2" y="3" rx="2"/><path d="M8 21h8m-4-4v4"/></g>'},Ra={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M4.93 4.93l1.41 1.41m11.32 11.32l1.41 1.41M2 12h2m16 0h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/></g>'};var Ua=Object.defineProperty,x=(t,e,i,a)=>{for(var s=void 0,n=t.length-1,r;n>=0;n--)(r=t[n])&&(s=r(e,i,s)||s);return s&&Ua(e,i,s),s};const Ee=["system","dark","bright"],Je={system:Na,dark:Ma,bright:Ra},W={overview:"/",alerts:"/alerts",cluster:"/cluster",trash:"/trash",manage:"/admin/manage",changePassword:"/admin/change-password",users:"/admin/users",apiTokens:"/admin/api-tokens"};function Wt(){return Object.entries(W).find(([,t])=>t===window.location.pathname)?.[0]??"overview"}function qa(){const t=localStorage.getItem("upgrid-theme");return Ee.includes(t)?t:"system"}class v extends L{constructor(){super(...arguments),this.targets=[],this.trashedTargets=[],this.channels=[],this.alerts=[],this.transitions=[],this.secrets=[],this.joinTokens=[],this.identities=[],this.apiTokens=[],this.authReady=!1,this.newApiToken="",this.error="",this.targetError="",this.live=!1,this.saving=!1,this.historyLoading=!1,this.joinUrl="",this.alertSearch="",this.alertDeliveryFilter="all",this.alertKindFilter="all",this.alertAcknowledgedFilter="all",this.search="",this.statusFilter="all",this.sort="name",this.selectedIds=new Set,this.activeSection=Wt(),this.copied=!1,this.setupMode=!1,this.warningDismissed=sessionStorage.getItem("upgrid-warning-dismissed")==="1",this.unlimitedUses=!1,this.theme=qa(),this.detailDirty=!1,this.detailTab="details",this.publicStatusGeneration=0,this.detailInitialState="",this.systemTheme=matchMedia("(prefers-color-scheme: light)"),this.systemThemeChanged=()=>{this.theme==="system"&&this.applyTheme()},this.routeChanged=()=>{if(this.setupMode&&this.setup){window.history.replaceState(null,"",this.setup.path);return}this.activeSection=Wt()},this.backgroundClicked=e=>{const i=this.renderRoot.querySelector(".account-menu");i?.open&&!e.composedPath().includes(i)&&(i.open=!1)},this.sessionExpired=()=>{this.events?.close(),this.events=void 0,this.stopPublicStatus(),this.session=void 0,this.settings=void 0,this.setupMode=!1,this.saving=!1,this.error="",this.targetError="",this.activeSection="overview",window.history.replaceState(null,"","/")}}connectedCallback(){super.connectedCallback(),this.applyTheme(),this.systemTheme.addEventListener("change",this.systemThemeChanged),window.addEventListener("popstate",this.routeChanged),document.addEventListener("pointerdown",this.backgroundClicked),window.addEventListener(Xe,this.sessionExpired),this.start()}disconnectedCallback(){this.systemTheme.removeEventListener("change",this.systemThemeChanged),window.removeEventListener("popstate",this.routeChanged),document.removeEventListener("pointerdown",this.backgroundClicked),window.removeEventListener(Xe,this.sessionExpired),this.events?.close(),this.stopPublicStatus(),super.disconnectedCallback()}async start(){try{const e=await f("/api/v1/setup");e.cluster_ready&&(this.session=await f("/api/v1/auth/session")),await this.activate(e)}catch(e){if(e instanceof B&&e.status===401&&window.location.pathname==="/")try{await this.activatePublicStatus()}catch(i){(!(i instanceof B)||i.status!==401)&&(this.error=i instanceof Error?i.message:String(i))}else this.error=e instanceof Error?e.message:String(e)}this.authReady=!0}async activatePublicStatus(){const e=++this.publicStatusGeneration,i=await f("/api/v1/status");e===this.publicStatusGeneration&&(this.publicStatus=i,this.live=!0,this.publicStatusTimer!==void 0&&window.clearInterval(this.publicStatusTimer),this.publicStatusTimer=window.setInterval(()=>{this.refreshPublicStatus()},3e4))}async refreshPublicStatus(){const e=++this.publicStatusGeneration;try{const i=await f("/api/v1/status");if(e!==this.publicStatusGeneration)return;this.publicStatus=i,this.live=!0}catch(i){if(e!==this.publicStatusGeneration)return;this.live=!1,i instanceof B&&i.status===401&&this.stopPublicStatus()}}stopPublicStatus(){this.publicStatusGeneration+=1,this.publicStatusTimer!==void 0&&window.clearInterval(this.publicStatusTimer),this.publicStatusTimer=void 0,this.publicStatus=void 0,this.live=!1}showLogin(){this.stopPublicStatus()}async activate(e){if(this.setup=e,this.setupMode=e.setup,this.setupMode){window.history.replaceState(null,"",e.path),e.cluster_ready?(await this.refresh(),this.connectEvents()):this.live=!0;return}await this.refresh(),this.connectEvents()}async login(e){e.preventDefault();const i=new FormData(e.currentTarget);this.saving=!0,this.error="";try{this.session=await f("/api/v1/auth/login",{method:"POST",body:JSON.stringify({username:String(i.get("username")??""),password:String(i.get("password")??"")})}),this.stopPublicStatus(),await this.activate(await f("/api/v1/setup"))}catch(a){this.error=a instanceof Error?a.message:String(a)}finally{this.saving=!1}}async logout(){await f("/api/v1/auth/logout",{method:"POST"}),this.events?.close(),this.stopPublicStatus(),this.session=void 0,this.settings=void 0,this.setupMode=!1,window.history.replaceState(null,"","/");try{await this.activatePublicStatus()}catch(e){(!(e instanceof B)||e.status!==401)&&(this.error=e instanceof Error?e.message:String(e))}}connectEvents(){this.events?.close(),this.events=new EventSource("/api/v1/events"),this.events.addEventListener("state",()=>{this.refresh()}),this.events.onopen=()=>this.live=!0,this.events.onerror=()=>this.live=!1}applyTheme(){const e=this.theme==="system"?this.systemTheme.matches?"bright":"dark":this.theme;this.dataset.theme=e,document.querySelector('meta[name="theme-color"]')?.setAttribute("content",e==="bright"?"#f4f8f6":"#0b1110")}cycleTheme(){this.theme=Ee[(Ee.indexOf(this.theme)+1)%Ee.length],localStorage.setItem("upgrid-theme",this.theme),this.applyTheme()}dismissWarning(){sessionStorage.setItem("upgrid-warning-dismissed","1"),this.warningDismissed=!0}async refresh(){try{[this.targets,this.trashedTargets,this.channels,this.alerts,this.transitions,this.secrets,this.cluster,this.joinTokens,this.identities,this.apiTokens,this.settings]=await Promise.all([f("/api/v1/targets"),f("/api/v1/trash/targets"),f("/api/v1/channels"),f("/api/v1/alerts"),f("/api/v1/transitions"),f("/api/v1/secrets"),f("/api/v1/cluster"),f("/api/v1/join-tokens"),f("/api/v1/identities"),f("/api/v1/api-tokens"),f("/api/v1/settings")]),this.error=""}catch(e){this.error=e instanceof Error?e.message:String(e)}}openTargetDialog(){this.targetError="",this.renderRoot.querySelector("#target-dialog")?.showModal()}closeTargetDialog(){this.targetError="",this.renderRoot.querySelector("#target-dialog")?.close()}openTarget(e){this.targetError="",this.detailDirty=!1,this.detailTab="details",this.selected=e,this.targetHistory=void 0,this.historyLoading=!0,this.loadTargetHistory(e.id),this.updateComplete.then(()=>{const i=this.renderRoot.querySelector("#detail-dialog"),a=i?.querySelector("form");a&&(this.detailInitialState=this.detailFormState(a)),i?.showModal()})}async loadTargetHistory(e){try{const i=await f(`/api/v1/targets/${e}/history?limit=720`);this.selected?.id===e&&(this.targetHistory=i)}catch(i){this.selected?.id===e&&(this.error=i instanceof Error?i.message:String(i))}finally{this.selected?.id===e&&(this.historyLoading=!1)}}closeDetailDialog(){this.targetError="",this.renderRoot.querySelector("#detail-dialog")?.close(),this.detailDirty=!1,this.detailTab="details",this.detailInitialState="",this.selected=void 0,this.targetHistory=void 0,this.historyLoading=!1}showDialog(e){this.renderRoot.querySelector(`#${e}`)?.showModal()}dismissOnBackdrop(e){const i=e.currentTarget;e.target===i&&(i.close(),i.id==="detail-dialog"&&this.closeDetailDialog())}navigate(e,i){e.preventDefault(),this.activeSection=i,window.history.pushState(null,"",W[i]),this.renderRoot.querySelector(".account-menu")?.removeAttribute("open")}closeDialog(e){this.renderRoot.querySelector(`#${e}`)?.close()}selectDetailTab(e){this.detailTab=e}toggleMaxRedirects(e){const i=e.currentTarget,a=i.form?.elements.namedItem("max_redirects");a&&(a.disabled=!i.checked),i.form&&this.compareDetailForm(i.form)}detailFormState(e){return JSON.stringify([...new FormData(e).entries()])}compareDetailForm(e){this.detailDirty=this.detailFormState(e)!==this.detailInitialState}updateDetailDirty(e){this.targetError="",this.compareDetailForm(e.currentTarget)}}x([g()],v.prototype,"targets");x([g()],v.prototype,"trashedTargets");x([g()],v.prototype,"channels");x([g()],v.prototype,"alerts");x([g()],v.prototype,"transitions");x([g()],v.prototype,"secrets");x([g()],v.prototype,"cluster");x([g()],v.prototype,"joinTokens");x([g()],v.prototype,"identities");x([g()],v.prototype,"apiTokens");x([g()],v.prototype,"settings");x([g()],v.prototype,"publicStatus");x([g()],v.prototype,"session");x([g()],v.prototype,"authReady");x([g()],v.prototype,"newApiToken");x([g()],v.prototype,"editingIdentity");x([g()],v.prototype,"error");x([g()],v.prototype,"targetError");x([g()],v.prototype,"live");x([g()],v.prototype,"saving");x([g()],v.prototype,"selected");x([g()],v.prototype,"targetHistory");x([g()],v.prototype,"historyLoading");x([g()],v.prototype,"editingChannel");x([g()],v.prototype,"joinUrl");x([g()],v.prototype,"alertSearch");x([g()],v.prototype,"alertDeliveryFilter");x([g()],v.prototype,"alertKindFilter");x([g()],v.prototype,"alertAcknowledgedFilter");x([g()],v.prototype,"search");x([g()],v.prototype,"statusFilter");x([g()],v.prototype,"sort");x([g()],v.prototype,"selectedIds");x([g()],v.prototype,"activeSection");x([g()],v.prototype,"copied");x([g()],v.prototype,"setupMode");x([g()],v.prototype,"setup");x([g()],v.prototype,"warningDismissed");x([g()],v.prototype,"unlimitedUses");x([g()],v.prototype,"theme");x([g()],v.prototype,"detailDirty");x([g()],v.prototype,"detailTab");class Fa extends v{async createTarget(e){e.preventDefault();const i=e.currentTarget,a=new FormData(i),s=i.querySelector("http-assertion-editor")?.value??[],n=et(a,a.getAll("channel_id").map(String),a.get("use_default_channels")==="on",void 0,s);this.targetError="",this.saving=!0;try{await f("/api/v1/targets",{method:"POST",body:JSON.stringify(n)}),i.reset(),this.closeTargetDialog(),await this.refresh()}catch(r){this.targetError=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}async updateTarget(e){if(e.preventDefault(),!this.selected)return;const i=new FormData(e.currentTarget),a=e.currentTarget.querySelector("http-assertion-editor")?.value??[];let s=`/api/v1/nodes/${this.selected.id}`,n={name:String(i.get("name"))};if(this.selected.kind==="http"){const r=i.get("follow_redirects")==="on";s=`/api/v1/targets/${this.selected.id}`,n={name:String(i.get("name")),kind:"http",url:String(i.get("url")),method:String(i.get("method")),accepted_statuses:String(i.get("statuses")).split(",").map(o=>{const[l,d]=o.trim().split("-").map(Number);return{start:l,end:d||l}}),follow_redirects:r,max_redirects:r?Number(i.get("max_redirects")):0,interval_seconds:Number(i.get("interval")),timeout_seconds:Number(i.get("timeout")),failure_threshold:Number(i.get("failures")),locations:Number(i.get("locations")),headers:Object.fromEntries(Object.entries(this.selected.headers).map(([o,l])=>[o,l.kind==="literal"?l.value:{secret_id:l.secret_id}])),body:this.selected.body?.kind==="literal"?this.selected.body.value:this.selected.body?{secret_id:this.selected.body.secret_id}:null,assertions:a,skip_tls_verification:i.get("skip_tls_verification")==="on",tls_ca_secret_id:String(i.get("tls_ca_secret_id")??"")||null,tls_client_certificate_secret_id:String(i.get("tls_client_certificate_secret_id")??"")||null,tls_client_private_key_secret_id:String(i.get("tls_client_private_key_secret_id")??"")||null,notification_channel_ids:i.getAll("channel_id").map(String),use_default_channels:i.get("use_default_channels")==="on"}}this.selected.kind!=="http"&&this.selected.kind!=="node"&&(s=`/api/v1/targets/${this.selected.id}`,n=et(i,i.getAll("channel_id").map(String),i.get("use_default_channels")==="on",this.selected.kind,a)),this.targetError="",this.saving=!0;try{await f(s,{method:"PUT",body:JSON.stringify(n)}),this.closeDetailDialog(),await this.refresh()}catch(r){this.targetError=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}async deleteTarget(){if(!(!this.selected||!window.confirm("Move this target and its history to trash? You can restore it before its retention period expires."))){this.saving=!0;try{await f(`/api/v1/targets/${this.selected.id}`,{method:"DELETE"}),this.closeDetailDialog(),await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async restoreTarget(e){window.confirm(`Restore ${e.name} with its settings and history?`)&&await this.saveResource(()=>f(`/api/v1/trash/targets/${e.id}/restore`,{method:"POST"}))}async purgeTarget(e){window.confirm(`Permanently delete ${e.name} and all of its history? This cannot be undone.`)&&await this.saveResource(()=>f(`/api/v1/trash/targets/${e.id}`,{method:"DELETE"}))}async setPaused(e){if(this.selected){this.saving=!0;try{await f(`/api/v1/targets/${this.selected.id}/${e?"pause":"resume"}`,{method:"POST"}),this.closeDetailDialog(),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}}async createSecret(e){e.preventDefault();const i=e.currentTarget,a=new FormData(i);this.saving=!0;try{await f("/api/v1/secrets",{method:"POST",body:JSON.stringify({name:a.get("name"),value:a.get("value")})}),i.reset(),this.closeDialog("secret-dialog"),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}openChannelDialog(e){this.editingChannel=e,this.showDialog("channel-dialog")}async channelSaved(e){const i=e.detail;this.channels=this.channels.some(({id:a})=>a===i.id)?this.channels.map(a=>a.id===i.id?i:a):[...this.channels,i],this.editingChannel=void 0,this.closeDialog("channel-dialog"),await this.refresh()}async setChannelDefault(e,i){try{await f(`/api/v1/channels/${e.id}/default`,{method:"PUT",body:JSON.stringify({default:i})}),await this.refresh()}catch(a){this.error=a instanceof Error?a.message:String(a)}}openTokenDialog(){this.unlimitedUses=!1,this.showDialog("token-config-dialog")}async createJoinToken(e){e.preventDefault();const i=new FormData(e.currentTarget);this.saving=!0;try{const a=await f("/api/v1/join-tokens",{method:"POST",body:JSON.stringify({expires_in_seconds:Number(i.get("expiration_days"))*86400,max_uses:this.unlimitedUses?null:Number(i.get("max_uses"))})});this.joinUrl=a.url,this.copied=!1,await this.refresh(),this.closeDialog("token-config-dialog"),this.showDialog("join-dialog")}catch(a){this.error=a instanceof Error?a.message:String(a)}finally{this.saving=!1}}passwordsMatch(e){const i=e.elements.namedItem("password"),a=e.elements.namedItem("password_confirmation");return!i||!a?!0:(a.setCustomValidity(i.value===a.value?"":"Passwords do not match."),e.reportValidity())}async createIdentity(e){e.preventDefault();const i=e.currentTarget;if(!this.passwordsMatch(i))return;const a=new FormData(i);await this.saveResource(async()=>{await f("/api/v1/identities",{method:"POST",body:JSON.stringify({username:String(a.get("username")??""),password:String(a.get("password")??"")})}),i.reset(),this.closeDialog("add-user-dialog")})}async updateIdentity(e,i){i.preventDefault();const a=i.currentTarget;if(!this.passwordsMatch(a))return;const s=new FormData(a),n=String(s.get("password")??"");await this.saveResource(async()=>{await f(`/api/v1/identities/${e.id}`,{method:"PUT",body:JSON.stringify({username:String(s.get("username")??""),password:n||null})}),e.id===this.session?.identity_id&&n?await this.logout():(this.closeDialog("edit-user-dialog"),this.editingIdentity=void 0)})}async deleteIdentity(e){window.confirm(`Delete identity ${e.username}? Its API Tokens will also be revoked.`)&&await this.saveResource(()=>f(`/api/v1/identities/${e.id}`,{method:"DELETE"}))}async createApiToken(e){e.preventDefault();const i=e.currentTarget,a=new FormData(i);await this.saveResource(async()=>{const s=Number(a.get("expires_in_days")),n=await f("/api/v1/api-tokens",{method:"POST",body:JSON.stringify({name:String(a.get("name")??""),expires_in_seconds:s?s*86400:null})});this.newApiToken=n.value,i.reset(),this.closeDialog("api-token-dialog")})}async revokeApiToken(e){window.confirm(`Revoke API token ${e.name}?`)&&await this.saveResource(()=>f(`/api/v1/api-tokens/${e.id}`,{method:"DELETE"}))}async setNodeDrain(e,i){await this.saveResource(()=>f(`/api/v1/nodes/${e.id}/drain`,{method:"PUT",body:JSON.stringify({draining:i,force:!1})}))}async removeNode(e,i){const a=i?`Replace failed node ${e.name}? Confirm that it is permanently stopped. Its assignments will be released immediately.`:`Remove drained node ${e.name} from the cluster?`;window.confirm(a)&&(await this.saveResource(()=>f(`/api/v1/nodes/${e.id}?force=${i}`,{method:"DELETE"})),i&&!this.error&&this.openTokenDialog())}async acknowledgeAlert(e){await this.updateAlert("acknowledge",e)}async retryAlert(e){await this.updateAlert("retry",e)}async updateAlert(e,i){await this.saveResource(()=>f(`/api/v1/alerts/${e}`,{method:"POST",body:JSON.stringify({target_id:i.target_id,channel_id:i.channel_id,scheduled_at_ms:i.scheduled_at_ms,kind:i.kind})}))}async updateSettings(e){e.preventDefault();const i=new FormData(e.currentTarget);await this.saveResource(()=>f("/api/v1/settings",{method:"PUT",body:JSON.stringify({public_status_enabled:i.get("public_status_enabled")==="on"})}))}async saveResource(e){this.saving=!0,this.error="";try{await e(),this.session&&await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}async setupChanged(e){const i=e.detail;if(this.setup=i,this.setupMode=i.setup,window.history.replaceState(null,"",i.path),i.setup){i.cluster_ready&&(this.session=await f("/api/v1/auth/session"),await this.refresh(),this.connectEvents());return}this.activeSection="overview",await this.refresh(),this.connectEvents()}async revokeJoinToken(e){if(window.confirm("Revoke this join token? Nodes using it will no longer be admitted.")){this.saving=!0;try{await f(`/api/v1/join-tokens/${e.id}`,{method:"DELETE"}),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}}async copyJoinUrl(){let e=!1;try{await navigator.clipboard.writeText(this.joinUrl),e=!0}catch{const i=Object.assign(document.createElement("textarea"),{value:this.joinUrl});i.style.cssText="position: fixed; opacity: 0",document.body.append(i),i.select(),e=document.execCommand("copy"),i.remove()}if(!e){this.error="Could not copy the join URL";return}this.copied=!0,window.setTimeout(()=>this.copied=!1,2e3)}toggleSelected(e,i){const a=new Set(this.selectedIds);i?a.add(e):a.delete(e),this.selectedIds=a}async bulkPause(e){this.saving=!0;try{await Promise.all([...this.selectedIds].map(i=>f(`/api/v1/targets/${i}/${e?"pause":"resume"}`,{method:"POST"}))),this.selectedIds=new Set,await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}async bulkDelete(){if(window.confirm(`Move ${this.selectedIds.size} selected Targets and their history to Trash?`)){this.saving=!0;try{await Promise.all([...this.selectedIds].map(e=>f(`/api/v1/targets/${e}`,{method:"DELETE"}))),this.selectedIds=new Set,await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async cleanupSecrets(){const e=this.secrets.filter(i=>!i.referenced);!e.length||!window.confirm(`Permanently delete ${e.length} unused ${e.length===1?"Secret":"Secrets"}? References are checked again when cleanup commits.`)||await this.saveResource(()=>f("/api/v1/secrets/unreferenced",{method:"DELETE"}))}async deleteResource(e,i,a){if(window.confirm(`Delete ${a}?`))try{await f(`/api/v1/${e}/${i}`,{method:"DELETE"}),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}}}function Si(t,e){return c`
    <div class="brand">
      <a class="brand-link" href="/" aria-label="UpGrid overview" @click=${e??h}><img src="/favicon.svg" alt="UpGrid" /></a>
      <span class="live"><i class=${`status-dot${t?" online":""}`}></i>${t?"Online":"Offline"}</span>
    </div>
  `}const za={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 3a2.85 2.83 0 1 1 4 4L7.5 20.5L2 22l1.5-5.5Zm-2 2l4 4"/>'};function Ha(t,e){const i=e.search.trim().toLocaleLowerCase();return(!i||`${t.target_name} ${t.channel_name}`.toLocaleLowerCase().includes(i))&&(e.delivery==="all"||t.delivery===e.delivery)&&(e.kind==="all"||t.kind===e.kind)&&(e.acknowledged==="all"||(e.acknowledged==="yes"?t.acknowledged_at_ms!==null:t.acknowledged_at_ms===null))}function Ba(t){return t.delivery==="pending"?t.next_attempt_at_ms===null?`${t.attempts} attempts`:`${t.attempts} attempts · next ${new Date(t.next_attempt_at_ms).toLocaleString()}`:t.delivery==="failed"?t.diagnostic??"Delivery failed":t.completed_at_ms===null?"Delivered":`Delivered ${new Date(t.completed_at_ms).toLocaleString()}`}function Va(t,e,i,a,s,n){const r=t.filter(o=>Ha(o,a));return c`
    <section class="heading" id="alerts">
      <div><span class="eyebrow">Delivery history</span><h1>Alerts</h1></div>
      <button class="button" @click=${n.create}>Add channel</button>
    </section>
    ${T({title:"Notification deliveries",label:"Alert history",metadata:`${r.length} of ${t.length} alerts`,className:"alert-history",content:c`
        <div class="alert-filters">
          <label>Search<input type="search" .value=${a.search} placeholder="Target or channel" @input=${o=>n.setSearch(o.target.value)} /></label>
          <label>Delivery<select .value=${a.delivery} @change=${o=>n.setDelivery(o.target.value)}><option value="all">All</option><option value="pending">Pending</option><option value="delivered">Delivered</option><option value="failed">Failed</option></select></label>
          <label>Transition<select .value=${a.kind} @change=${o=>n.setKind(o.target.value)}><option value="all">All</option><option value="down">Down</option><option value="recovered">Recovered</option></select></label>
          <label>Acknowledged<select .value=${a.acknowledged} @change=${o=>n.setAcknowledged(o.target.value)}><option value="all">All</option><option value="no">No</option><option value="yes">Yes</option></select></label>
        </div>
        ${r.length?r.map(o=>c`
                  <div class="resource alert-resource">
                    <div class="alert-summary">
                      <div class="channel-title">
                        <strong>${o.target_name}</strong>
                        <span class=${`badge ${o.kind==="recovered"?"up":"down"}`}>${o.kind}</span>
                        <span class="badge">${o.delivery}</span>
                        ${o.acknowledged_at_ms===null?h:c`<span class="badge">acknowledged</span>`}
                      </div>
                      <code>${o.channel_name} · ${new Date(o.scheduled_at_ms).toLocaleString()}</code>
                      <span class="meta">${Ba(o)}</span>
                    </div>
                    <div class="alert-actions">
                      ${o.delivery==="failed"?c`<button class="button secondary" ?disabled=${s} @click=${()=>n.retry(o)}>Retry</button>`:h}
                      ${o.acknowledged_at_ms===null?c`<button class="button secondary" ?disabled=${s} @click=${()=>n.acknowledge(o)}>Acknowledge</button>`:h}
                    </div>
                  </div>
                `):c`<upgrid-empty-state>No alerts match these filters</upgrid-empty-state>`}
      `})}
    <div class="page-columns">
      ${T({title:"Availability transitions",label:"Availability history",metadata:`${e.length} events`,content:c`
          ${e.length?e.map(o=>{const l=o.kind==="recovered"?"up":"down";return c`
                    <div class="resource">
                      <div class="transition-main">
                        <span class=${`state ${l}`} aria-hidden="true"></span>
                        <div>
                          <strong>${o.target_name}</strong>
                          <code>${new Date(o.scheduled_at_ms).toLocaleString()}</code>
                        </div>
                      </div>
                      <span class=${`badge ${l}`}>${o.kind}</span>
                    </div>
                  `}):c`<upgrid-empty-state>No availability transitions</upgrid-empty-state>`}
        `})}
      ${T({title:"Notification channels",metadata:`${i.length} configured`,content:c`
          ${i.length?i.map(o=>c`
                    <div class="resource channel-resource">
                      <div class="channel-summary"><div class="channel-title"><strong>${o.name}</strong><span class="badge">${o.kind}</span></div><code>${o.destination}</code></div>
                      <div class="channel-actions">
                        <label class="switch"><span>Default</span><input class="switch-control" type="checkbox" role="switch" aria-label=${`Default channel ${o.name}`} .checked=${o.default} @change=${l=>n.setDefault(o,l.target.checked)} /></label>
                        <button class="button secondary icon-button" aria-label=${`Edit channel ${o.name}`} title=${`Edit ${o.name}`} @click=${()=>n.edit(o)}><iconify-icon .icon=${za} aria-hidden="true"></iconify-icon></button>
                        <button class="button danger icon-button" aria-label=${`Delete channel ${o.name}`} title=${`Delete ${o.name}`} @click=${()=>n.remove(o)}><iconify-icon .icon=${ne} aria-hidden="true"></iconify-icon></button>
                      </div>
                    </div>
                  `):c`<upgrid-empty-state>No notification channels</upgrid-empty-state>`}
        `})}
    </div>
  `}function it(t){const e=t.currentTarget,i=e.elements.namedItem("password"),a=e.elements.namedItem("password_confirmation");!(i instanceof HTMLInputElement)||!(a instanceof HTMLInputElement)||a.setCustomValidity(a.value&&a.value!==i.value?"Passwords do not match.":"")}function Ja(t,e,i,a){return c`
    <main class="shell setup-shell">
      <header>
        ${Si(t)}
      </header>
      ${T({label:"Sign in",className:"auth-panel",content:c`
          <form class="choice" @submit=${a.login} @input=${a.changed}>
            <div><span class="eyebrow">Cluster access</span><h1 id="login-title">Sign in</h1><p class="meta">Use a replicated operator identity.</p></div>
            ${i?c`<div class="notice" role="alert">${i}</div>`:h}
            <label>Username<input name="username" autocomplete="username" required autofocus /></label>
            <label>Password<input name="password" type="password" autocomplete="current-password" required /></label>
            <div class="dialog-actions">${P({label:e?"Signing in...":"Sign in",busy:e,error:i})}</div>
          </form>
        `})}
    </main>`}function Ga(t,e,i,a){return t?c`
    <div class="admin-page change-password-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>Change password</h1></div></div>
      ${T({className:"auth-panel",content:c`
          <form class="choice" @submit=${s=>a.updateIdentity(t,s)} @input=${s=>{it(s),a.changed()}}>
            <input name="username" type="hidden" .value=${t.username} />
            <label>Username<input .value=${t.username} autocomplete="username" disabled /></label>
            <label>New password<input name="password" type="password" minlength="12" autocomplete="new-password" required autofocus /></label>
            <label>Confirm new password<input name="password_confirmation" type="password" minlength="12" autocomplete="new-password" required /></label>
            <div class="dialog-actions">${P({label:"Change password",busy:e,error:i})}</div>
          </form>
        `})}
    </div>`:c`<upgrid-empty-state>Current identity unavailable</upgrid-empty-state>`}function Ka(t,e,i,a,s,n){return c`
    <div class="admin-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>Users</h1></div><button class="button" type="button" @click=${n.openAddUser}>Add user</button></div>
      ${T({title:"Operator identities",metadata:`${t.length} administrators`,content:c`
          ${t.map(r=>c`
              <div class="resource user-resource">
                <button class="resource-main" type="button" aria-label=${`Edit user ${r.username}`} ?disabled=${a} @click=${()=>n.openEditUser(r)}>
                  <span>
                    <strong>${r.username}</strong>
                    <code>Operator identity${r.id===e?" · current user":""}</code>
                  </span>
                </button>
                <button class="button danger icon-button" type="button" aria-label=${`Delete user ${r.username}`} title=${`Delete ${r.username}`} ?disabled=${r.id===e||a} @click=${()=>n.deleteIdentity(r)}><iconify-icon .icon=${ne} aria-hidden="true"></iconify-icon></button>
              </div>
            `)}
        `})}
    </div>
    <dialog id="add-user-dialog" aria-labelledby="add-user-title" @click=${n.dismissDialog}>
      <div class="dialog-head"><h2 id="add-user-title">Add user</h2></div>
      <form @submit=${n.createIdentity} @input=${r=>{it(r),n.changed()}}>
        <label>Username<input name="username" autocomplete="username" required autofocus /></label>
        <label>Password<input name="password" type="password" minlength="12" autocomplete="new-password" required /></label>
        <label>Confirm password<input name="password_confirmation" type="password" minlength="12" autocomplete="new-password" required /></label>
        <div class="dialog-actions"><button class="button secondary" type="button" @click=${n.closeAddUser}>Cancel</button>${P({label:a?"Adding...":"Add user",busy:a,error:s})}</div>
      </form>
    </dialog>
    ${i?c`
          <dialog id="edit-user-dialog" aria-labelledby="edit-user-title" @click=${n.dismissDialog}>
            <div class="dialog-head"><h2 id="edit-user-title">Edit user</h2></div>
            <form @submit=${r=>n.updateIdentity(i,r)} @input=${r=>{it(r),n.changed()}}>
              <label>Username<input name="username" .value=${i.username} autocomplete="username" required autofocus /></label>
              <label>New password<input name="password" type="password" minlength="12" autocomplete="new-password" placeholder="Keep current password" /></label>
              <label>Confirm new password<input name="password_confirmation" type="password" minlength="12" autocomplete="new-password" placeholder="Keep current password" /></label>
              <div class="dialog-actions"><button class="button secondary" type="button" @click=${n.closeEditUser}>Cancel</button>${P({label:"Save changes",busy:a,error:s,trackChanges:!0})}</div>
            </form>
          </dialog>`:h}`}function Wa(t,e,i,a,s){return c`
    <div class="admin-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>API tokens</h1></div><button class="button" type="button" @click=${s.openApiToken}>New token</button></div>
      ${T({title:"API tokens",metadata:`${t.length} active`,content:c`
          ${e?c`<div class="notice token-value" role="status"><strong>Copy this token now.</strong><code>${e}</code><button class="button secondary" @click=${s.dismissToken}>Dismiss</button></div>`:h}
          ${t.length?t.map(n=>c`<div class="resource"><div><strong>${n.name}</strong><code>${n.expires_at_ms?`Expires ${new Date(n.expires_at_ms).toLocaleString()}`:"Never expires"}</code></div><button class="button danger" @click=${()=>s.revokeApiToken(n)}>Revoke</button></div>`):c`<upgrid-empty-state>No API tokens</upgrid-empty-state>`}
        `})}
    </div>
    <dialog id="api-token-dialog" aria-labelledby="api-token-title" @click=${s.dismissDialog}>
      <div class="dialog-head"><h2 id="api-token-title">New API token</h2></div>
      <form @submit=${s.createApiToken} @input=${s.changed}>
        <label>Name<input name="name" placeholder="Automation" required autofocus /></label>
        <label>Expires in days<input name="expires_in_days" type="number" min="1" max="365" placeholder="Never" /></label>
        <div class="dialog-actions"><button class="button secondary" type="button" @click=${s.closeApiToken}>Cancel</button>${P({label:i?"Creating...":"Create API token",busy:i,error:a})}</div>
      </form>
    </dialog>`}function Qa(t,e,i,a,s){return c`
    <div class="admin-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>Manage</h1></div></div>
      ${T({title:"Public status access",content:c`
          <form @submit=${a} @input=${s}>
            <label class="switch">
              <span class="setting-copy">
                Allow status viewing without login
                <small>External visitors can see target names, states, and recent evaluation metrics. URLs, configuration, alerts, cluster data, and administration remain private.</small>
              </span>
              <input
                class="switch-control"
                name="public_status_enabled"
                type="checkbox"
                role="switch"
                .checked=${t?.public_status_enabled??!1}
                ?disabled=${t===void 0||e}
              />
            </label>
            <div class="dialog-actions">${P({label:e?"Saving...":"Save changes",busy:e,blocked:t===void 0,error:i,baselineKey:String(t?.public_status_enabled),blockedMessage:"Settings are unavailable",trackChanges:!0})}</div>
          </form>
        `})}
    </div>`}const Ya={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><path d="M15 22v-4a4.8 4.8 0 0 0-1-3.5c3 0 6-2 6-5.5c.08-1.25-.27-2.48-1-3.5c.28-1.15.28-2.35 0-3.5c0 0-1 0-3 1.5c-2.64-.5-5.36-.5-8 0C6 2 5 2 5 2c-.3 1.15-.3 2.35 0 3.5A5.403 5.403 0 0 0 4 9c0 3.5 3 5.5 6 5.5c-.39.49-.68 1.05-.85 1.65c-.17.6-.22 1.23-.15 1.85v4"/><path d="M9 18c-4.51 2-5-2-7-2"/></g>'},Za={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><path d="M21.54 15H17a2 2 0 0 0-2 2v4.54M7 3.34V5a3 3 0 0 0 3 3v0a2 2 0 0 1 2 2v0c0 1.1.9 2 2 2v0a2 2 0 0 0 2-2v0c0-1.1.9-2 2-2h3.17M11 21.95V18a2 2 0 0 0-2-2v0a2 2 0 0 1-2-2v-1a2 2 0 0 0-2-2H2.05"/><circle cx="12" cy="12" r="10"/></g>'};function _e(){return c`
    <footer aria-label="Project information">
      <div class="footer-links">
        <a href="https://miao.dev">A project by Pop</a>
        <span aria-hidden="true">|</span>
        <a href="https://github.com/George-Miao/UpGrid">
          <iconify-icon .icon=${Ya} aria-hidden="true"></iconify-icon>GitHub
        </a>
        <span aria-hidden="true">|</span>
        <a href="https://upgrid.rs">
          <iconify-icon .icon=${Za} aria-hidden="true"></iconify-icon>upgrid.rs
        </a>
      </div>
      <div class="footer-powered">
        Proudly powered by <a href="https://compio.rs/">Compio</a> and
        <a href="https://github.com/databendlabs/openraft">OpenRaft</a>
      </div>
    </footer>
  `}function Ai(t,e=[],i=!0){return c`
    <div class="channel-fields">
      <label class="switch">
        <span>Use default channels</span>
        <input
          class="switch-control"
          name="use_default_channels"
          type="checkbox"
          role="switch"
          .checked=${i}
          @change=${s=>{const n=s.currentTarget;n.closest(".channel-fields")?.querySelectorAll('input[data-default="true"]').forEach(o=>{o.disabled=n.checked,o.checked=n.checked||o.dataset.explicit==="true"}),n.form?.dispatchEvent(new Event("input",{bubbles:!0}))}}
        />
      </label>
      <div class="channel-options">
        ${t.length?t.map(s=>{const n=e.includes(s.id),r=i&&s.default;return c`
                  <label class="checkbox-option">
                    <span class="switch-label">${s.name} <span class="badge">${s.kind}</span></span>
                    <input
                      class="checkbox-control"
                      name="channel_id"
                      type="checkbox"
                      value=${s.id}
                      data-default=${String(s.default)}
                      data-explicit=${String(n)}
                      .checked=${n||r}
                      ?disabled=${r}
                      @change=${o=>{const l=o.currentTarget;l.dataset.explicit=String(l.checked)}}
                    />
                  </label>
                `}):c`<upgrid-empty-state>No notification channels are available</upgrid-empty-state>`}
      </div>
    </div>`}function Ti(t,e=null,i=null,a=null){const s=n=>c`
    <option value="">Not configured</option>
    ${t.map(r=>c`<option value=${r.id} ?selected=${r.id===n}>${r.name}</option>`)}
  `;return c`
    <fieldset class="tls-fields">
      <legend>HTTPS trust and mutual TLS</legend>
      <label>Custom CA bundle secret<select name="tls_ca_secret_id">${s(e)}</select></label>
      <div class="row">
        <label>Client certificate secret<select name="tls_client_certificate_secret_id">${s(i)}</select></label>
        <label>Client private key secret<select name="tls_client_private_key_secret_id">${s(a)}</select></label>
      </div>
      <p class="meta">PEM values stay encrypted. Client certificate and private key must be configured together.</p>
    </fieldset>
  `}const Ci={http:"https://example.com/health",tcp:"database.internal:5432",dns:"service.internal",icmp:"192.0.2.10",tls:"example.com:443"};function Ei(t){return t.closest("dialog")?.querySelector(".form-tabs")}function gt(t,e){Ei(t)?.querySelectorAll("[role='tab']").forEach(i=>{const a=i.dataset.tab===e;i.setAttribute("aria-selected",String(a)),i.tabIndex=-1}),t.querySelectorAll("[role='tabpanel']").forEach(i=>{i.hidden=i.dataset.panel!==e})}function Xa(t){for(let e=0;e<t.elements.length;e+=1){const i=t.elements.item(e);if(i instanceof HTMLElement&&"checkValidity"in i&&typeof i.checkValidity=="function"&&!i.checkValidity())return i}}function Pi(t,e){t.preventDefault();const i=t.currentTarget,a=Xa(i);if(!a){e(t);return}const s=a.closest("[role='tabpanel']");s&&i.closest("dialog")?.querySelector(`[role='tab'][aria-controls='${s.id}']`)?.click(),queueMicrotask(()=>a.reportValidity())}function Di(t,e){const i=t.elements.namedItem("url");i&&(i.placeholder=Ci[e],i.type=e==="http"?"url":"text"),t.querySelectorAll("[data-http-only]").forEach(o=>{o.hidden=e!=="http"});const a=Ei(t),s=a?.querySelector("[role='tab'][aria-selected='true']")?.dataset.tab??"general",n=a?.querySelector("[data-tab='assertions']");n&&(n.disabled=e!=="http"),gt(t,e!=="http"&&s==="assertions"?"general":s);const r=t.elements.namedItem("method");r&&(r.disabled=e!=="http",r.disabled&&(r.value="GET"))}function er(t){const e=t.currentTarget;e.form&&Di(e.form,e.value)}function Se(t){const e=t.currentTarget;e.form&&e.dataset.tab&&gt(e.form,e.dataset.tab)}function tr(t){const e=t.currentTarget;queueMicrotask(()=>{Di(e,"http"),gt(e,"general")})}function ir(t,e,i,a,s){return c`
    <dialog id="target-dialog" aria-labelledby="add-target-title" @click=${s.backdrop}>
      <div class="dialog-head target-dialog-head">
        <h2 id="add-target-title">Add target</h2>
        <div class="form-tabs" role="tablist" aria-label="Target settings">
          <button id="target-general-tab" form="target-form" type="button" role="tab" data-tab="general" aria-controls="target-general-panel" aria-selected="true" tabindex="-1" @click=${Se}>General</button>
          <button id="target-assertions-tab" form="target-form" type="button" role="tab" data-tab="assertions" aria-controls="target-assertions-panel" aria-selected="false" tabindex="-1" @click=${Se}>Assertions</button>
          <button id="target-evaluation-tab" form="target-form" type="button" role="tab" data-tab="evaluation" aria-controls="target-evaluation-panel" aria-selected="false" tabindex="-1" @click=${Se}>Evaluation</button>
          <button id="target-notifications-tab" form="target-form" type="button" role="tab" data-tab="notifications" aria-controls="target-notifications-panel" aria-selected="false" tabindex="-1" @click=${Se}>Notifications</button>
        </div>
      </div>
      <form id="target-form" novalidate @submit=${n=>Pi(n,s.create)} @input=${s.changed} @reset=${tr}>
        <section id="target-general-panel" class="target-tab-panel" role="tabpanel" data-panel="general" aria-labelledby="target-general-tab">
          <label>Name<input name="name" placeholder="Production API" required autofocus /></label>
          <div class="row endpoint-row">
            <label>Type<select name="kind" @change=${er}><option value="http">HTTP</option><option value="tcp">TCP connect</option><option value="dns">DNS resolution</option><option value="icmp">ICMP echo</option><option value="tls">TLS certificate</option></select></label>
            <label>URL / endpoint<input name="url" type="url" placeholder=${Ci.http} required /></label>
          </div>
          <label data-http-only>Method<input name="method" value="GET" required /></label>
          <div data-http-only>${Ti(e)}</div>
        </section>
        <section id="target-assertions-panel" class="target-tab-panel" role="tabpanel" data-panel="assertions" data-http-only aria-labelledby="target-assertions-tab" hidden>
          <http-assertion-editor name="assertions" target-id="new"></http-assertion-editor>
        </section>
        <section id="target-evaluation-panel" class="target-tab-panel" role="tabpanel" data-panel="evaluation" aria-labelledby="target-evaluation-tab" hidden>
          <div class="row">
            <label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label>
            <label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label>
          </div>
          <div class="row">
            <label>Failures before down<input name="failures" type="number" min="1" value="3" required /></label>
            <label>Evaluation locations<input name="locations" type="number" min="1" max="32" value="1" required /></label>
          </div>
        </section>
        <section id="target-notifications-panel" class="target-tab-panel" role="tabpanel" data-panel="notifications" aria-labelledby="target-notifications-tab" hidden>
          ${Ai(t)}
        </section>
        ${a?c`<div class="notice" role="alert">${a}</div>`:h}
        <div class="dialog-actions">
          <button class="button secondary" type="button" @click=${s.close}>Cancel</button>
          ${P({label:i?"Creating...":"Create target",busy:i,error:a})}
        </div>
      </form>
    </dialog>`}function sr(t,e,i,a,s,n,r,o,l,d,p){const u=t.kind==="node",m=t.kind==="http",b=t.accepted_statuses.map(y=>y.start===y.end?y.start:`${y.start}-${y.end}`).join(","),$=t.history.slice(0,30).reverse(),k=Math.max(1,...$.map(y=>y.latency_ms)),_=e?.items??[],I=_.reduce((y,R)=>y+R.samples,0),N=_.reduce((y,R)=>y+R.successes,0),O=_.reduce((y,R)=>y+R.latency_total_ms,0),E=I?`${(N/I*100).toFixed(2)}%`:"—",le=new Map(r.map(y=>[y.id,y.name])),w=y=>new Date(y).toLocaleString(void 0,{month:"short",day:"numeric",hour:"2-digit",minute:"2-digit"}),A=y=>y>=1e3?`${(y/1e3).toFixed(y>=1e4?0:1)} s`:`${Math.round(y)} ms`,V=I?A(O/I):"—",J=[{id:"details",label:"Details"},{id:"general",label:"General"},...m?[{id:"assertions",label:"Assertions"}]:[],...u?[]:[{id:"evaluation",label:"Evaluation"},{id:"notifications",label:"Notifications"}]],C=J.some(({id:y})=>y===n)?n:"details";return c`
    <dialog id="detail-dialog" aria-labelledby="target-detail-title" @click=${p.backdrop}>
      <div class="dialog-head target-dialog-head detail-dialog-head">
        <h2 id="target-detail-title">${u?"Node details":"Target details"}</h2>
        <div class="form-tabs" role="tablist" aria-label=${`${u?"Node":"Target"} details`}>
          ${J.map(({id:y,label:R})=>c`<button form="detail-form" type="button" role="tab" aria-controls=${`target-${y}-panel`} aria-selected=${String(C===y)} tabindex="-1" @click=${()=>p.selectTab(y)}>${R}</button>`)}
        </div>
        <button class="button secondary icon-button dialog-close" type="button" aria-label=${`Close ${u?"Node":"Target"} details`} title="Close" @click=${p.close}><iconify-icon .icon=${si} aria-hidden="true"></iconify-icon></button>
      </div>
      <form id="detail-form" class="detail-form" novalidate @submit=${y=>Pi(y,p.update)} @input=${p.changed}>
        <section id="target-details-panel" class="target-tab-panel details-panel" role="tabpanel" aria-label="Details" ?hidden=${C!=="details"}>
          <section class="history">
            <div class="history-head"><h3>Long-term summary</h3><span class="meta">Last 30 days</span></div>
            ${i?c`<p class="meta">Loading long-term history…</p>`:I?c`
                    <div class="history-summary" aria-label="Long-term evaluation summary">
                      <div><span>Availability</span><strong>${E}</strong></div>
                      <div><span>Average latency</span><strong>${V}</strong></div>
                      <div><span>Evaluations</span><strong>${I.toLocaleString()}</strong></div>
                    </div>
                  `:c`<upgrid-empty-state>No long-term history recorded yet</upgrid-empty-state>`}
          </section>
          <section class="history">
            <div class="history-head"><h3>Evaluation history</h3>${$.length?c`<span class="meta">Latest ${$.length}</span>`:h}</div>
            ${$.length?c`
              <div class="chart-plot">
                <div class="chart-scale" aria-hidden="true"><span>${A(k)}</span><span>${A(k/2)}</span><span>0 ms</span></div>
                <div class="history-chart" role="list" aria-label=${`Recent evaluation latency, 0 to ${A(k)}`}>
                  ${$.map(y=>{const R=y.succeeded?"Passed":"Failed",Ii=u||!m?y.succeeded?"reachable":"unreachable":y.status_code===null?"network error":`HTTP ${y.status_code}`,Oi=le.get(y.executor_node_id)??`Node ${y.executor_node_id.slice(0,8)}`,mt=`${R} at ${new Date(y.recorded_at_ms).toLocaleString()}: ${y.latency_ms} ms, ${Ii}. Executed by ${Oi}`;return c`<span class="history-bar ${y.succeeded?"up":"down"}" role="listitem" aria-label=${mt} title=${mt} style=${`height: ${Math.max(8,y.latency_ms/k*100)}%`}></span>`})}
                </div>
              </div>
              <div class="chart-axis"><span>${w($[0].recorded_at_ms)}</span><span>${w($[$.length-1].recorded_at_ms)}</span></div>
              <div class="chart-legend"><span><i class="up"></i>Passed</span><span><i class="down"></i>Failed</span><span>Height = latency</span></div>
            `:c`<upgrid-empty-state>No evaluations recorded yet</upgrid-empty-state>`}
          </section>
        </section>
        <section id="target-general-panel" class="target-tab-panel" role="tabpanel" aria-label="General" ?hidden=${C!=="general"}>
          <label>Name<input name="name" .value=${t.name} required /></label>
          ${u?c`<label>RPC URL<input .value=${t.url} disabled /></label>`:c`
                <div class="row endpoint-row"><label>Type<input .value=${t.kind.toUpperCase()} disabled /></label><label>URL / endpoint<input name="url" .value=${t.url} required /></label></div>
                ${m?c`
                      <div class="row"><label>Method<input name="method" .value=${t.method} required /></label><label>Expected statuses<input name="statuses" .value=${b} required /></label></div>
                      <div class="row"><label class="switch"><span>Follow redirects</span><input class="switch-control" name="follow_redirects" type="checkbox" role="switch" .checked=${t.follow_redirects} @change=${p.redirects} /></label><label>Maximum redirects<input name="max_redirects" type="number" min="0" .value=${String(t.max_redirects)} ?disabled=${!t.follow_redirects} required /></label></div>
                      <label class="switch"><span>Skip TLS verification</span><input class="switch-control" name="skip_tls_verification" type="checkbox" role="switch" .checked=${t.skip_tls_verification} /></label>
                      ${Ti(l,t.tls_ca_secret_id,t.tls_client_certificate_secret_id,t.tls_client_private_key_secret_id)}
                    `:h}
              `}
        </section>
        ${m?c`<section id="target-assertions-panel" class="target-tab-panel" role="tabpanel" aria-label="Assertions" ?hidden=${C!=="assertions"}>
                <http-assertion-editor name="assertions" target-id=${t.id} .assertions=${t.assertions}></http-assertion-editor>
              </section>`:h}
        ${u?h:c`
              <section id="target-evaluation-panel" class="target-tab-panel" role="tabpanel" aria-label="Evaluation" ?hidden=${C!=="evaluation"}>
                <div class="row"><label>Interval (seconds)<input name="interval" type="number" min="1" .value=${String(t.interval_seconds)} required /></label><label>Timeout (seconds)<input name="timeout" type="number" min="1" .value=${String(t.timeout_seconds)} required /></label></div>
                <div class="row"><label>Failures before down<input name="failures" type="number" min="1" .value=${String(t.failure_threshold)} required /></label><label>Evaluation locations<input name="locations" type="number" min="1" max="32" .value=${String(t.locations)} required /></label></div>
              </section>
              <section id="target-notifications-panel" class="target-tab-panel" role="tabpanel" aria-label="Notifications" ?hidden=${C!=="notifications"}>
                ${Ai(o,t.notification_channel_ids,t.use_default_channels)}
              </section>
            `}
        ${d?c`<div class="notice" role="alert">${d}</div>`:h}
        ${C==="details"?u?h:c`<div class="dialog-actions"><div class="danger-actions">
                  <button class="button danger icon-button" type="button" aria-label="Move target to trash" title="Move to trash" @click=${p.delete}><iconify-icon .icon=${ne} aria-hidden="true"></iconify-icon></button>
                  <button class=${`button ${t.paused?"success":"warning"} icon-button`} type="button" aria-label=${t.paused?"Resume evaluations":"Pause evaluations"} title=${t.paused?"Resume evaluations":"Pause evaluations"} @click=${()=>p.pause(!t.paused)}><iconify-icon .icon=${t.paused?ii:ti} aria-hidden="true"></iconify-icon></button>
                </div></div>`:c`<div class="dialog-actions">${P({label:"Save changes",busy:a,changed:s,error:d,baselineKey:t.id})}</div>`}
      </form>
    </dialog>`}var ar=Object.getOwnPropertyDescriptor,rr=(t,e,i,a)=>{for(var s=a>1?void 0:a?ar(e,i):e,n=t.length-1,r;n>=0;n--)(r=t[n])&&(s=r(s)||s);return s};let st=class extends Fa{renderBrand(){return Si(this.live,t=>this.navigate(t,"overview"))}render(){const t=this.targets.filter(r=>r.availability==="up").length,e=this.targets.filter(r=>r.availability==="down").length,i=this.alerts.filter(r=>r.delivery==="pending").length,a=["overview","alerts","cluster","trash"],s=this.targets.filter(r=>`${r.name} ${r.url}`.toLowerCase().includes(this.search.toLowerCase())).filter(r=>this.statusFilter==="all"?!0:this.statusFilter==="paused"?r.paused:r.availability===this.statusFilter).sort((r,o)=>this.sort==="status"&&r.availability.localeCompare(o.availability)||r.name.localeCompare(o.name)),n={login:r=>{this.login(r)},logout:()=>{this.logout()},createIdentity:r=>{this.createIdentity(r)},openAddUser:()=>this.showDialog("add-user-dialog"),closeAddUser:()=>this.closeDialog("add-user-dialog"),openEditUser:r=>{this.editingIdentity=r,this.updateComplete.then(()=>this.showDialog("edit-user-dialog"))},closeEditUser:()=>{this.closeDialog("edit-user-dialog"),this.editingIdentity=void 0},openApiToken:()=>this.showDialog("api-token-dialog"),closeApiToken:()=>this.closeDialog("api-token-dialog"),dismissDialog:r=>this.dismissOnBackdrop(r),updateIdentity:(r,o)=>{this.updateIdentity(r,o)},deleteIdentity:r=>{this.deleteIdentity(r)},createApiToken:r=>{this.createApiToken(r)},revokeApiToken:r=>{this.revokeApiToken(r)},dismissToken:()=>this.newApiToken="",changed:()=>this.error=""};return this.authReady&&!this.setupMode&&!this.session&&!this.publicStatus?c`${Ja(this.live,this.saving,this.error,n)}${_e()}`:this.setupMode&&this.setup?c`
        <main class="shell setup-shell">
          <header>
            ${this.renderBrand()}
            <div></div>
            <div class="actions"><button class="button secondary icon-button" aria-label=${`Theme: ${this.theme}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${Je[this.theme]} aria-hidden="true"></iconify-icon></button></div>
          </header>
          ${this.error?c`<div class="notice" role="alert">${this.error}</div>`:h}
          <upgrid-setup .setup=${this.setup} @setup-changed=${this.setupChanged}></upgrid-setup>
        </main>${_e()}`:!this.session&&this.publicStatus?this.renderPublicStatusPage(this.publicStatus.targets):c`
      <main class="shell">
        <header>
          ${this.renderBrand()}
          <nav aria-label="Primary">
            ${a.map(r=>c`<a class=${this.activeSection===r?"active":""} href=${W[r]} @click=${o=>this.navigate(o,r)}>${r[0].toUpperCase()}${r.slice(1)}</a>`)}
          </nav>
          <div class="actions">
            <button class="button secondary icon-button" aria-label=${`Theme: ${this.theme}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${Je[this.theme]} aria-hidden="true"></iconify-icon></button>
            <details class="account-menu">
              <summary class="button secondary icon-button" aria-label=${`Account menu for ${this.session?.username}`} title=${`Account: ${this.session?.username}`}><iconify-icon .icon=${rs} aria-hidden="true"></iconify-icon></summary>
              <div class="account-dropdown" role="menu">
                <a class="button secondary" role="menuitem" href=${W.manage} @click=${r=>this.navigate(r,"manage")}>Manage</a>
                <a class="button secondary" role="menuitem" href=${W.changePassword} @click=${r=>this.navigate(r,"changePassword")}>Change password</a>
                <a class="button secondary" role="menuitem" href=${W.users} @click=${r=>this.navigate(r,"users")}>Manage user</a>
                <a class="button secondary" role="menuitem" href=${W.apiTokens} @click=${r=>this.navigate(r,"apiTokens")}>API token</a>
                <div class="account-separator" role="separator"></div>
                <button class="button danger" role="menuitem" type="button" @click=${()=>{this.logout()}}>Logout</button>
              </div>
            </details>
          </div>
        </header>
        ${this.error?c`<div class="notice" role="alert">${this.error}</div>`:h}
        ${this.setup?.warning&&!this.warningDismissed?c`<div class="notice" role="status">${this.setup.warning}<button class="button secondary" style="float: right; margin: -6px" @click=${this.dismissWarning}>Dismiss</button></div>`:h}
        ${this.activeSection==="overview"?this.renderOverview(s,t,e,i):this.activeSection==="alerts"?Va(this.alerts,this.transitions,this.channels,{search:this.alertSearch,delivery:this.alertDeliveryFilter,kind:this.alertKindFilter,acknowledged:this.alertAcknowledgedFilter},this.saving,{create:()=>this.openChannelDialog(),edit:r=>this.openChannelDialog(r),remove:r=>{this.deleteResource("channels",r.id,r.name)},setDefault:(r,o)=>{this.setChannelDefault(r,o)},acknowledge:r=>{this.acknowledgeAlert(r)},retry:r=>{this.retryAlert(r)},setSearch:r=>this.alertSearch=r,setDelivery:r=>this.alertDeliveryFilter=r,setKind:r=>this.alertKindFilter=r,setAcknowledged:r=>this.alertAcknowledgedFilter=r}):this.activeSection==="cluster"?this.renderClusterPage():this.activeSection==="trash"?this.renderTrashPage():this.activeSection==="manage"?Qa(this.settings,this.saving,this.error,r=>{this.updateSettings(r)},()=>this.error=""):this.activeSection==="changePassword"?Ga(this.identities.find(r=>r.id===this.session?.identity_id),this.saving,this.error,n):this.activeSection==="users"?Ka(this.identities,this.session?.identity_id,this.editingIdentity,this.saving,this.error,n):Wa(this.apiTokens,this.newApiToken,this.saving,this.error,n)}
      </main>${_e()}
      ${ir(this.channels,this.secrets,this.saving,this.targetError,{backdrop:r=>this.dismissOnBackdrop(r),close:()=>this.closeTargetDialog(),create:r=>{this.createTarget(r)},changed:()=>this.targetError=""})}
      ${this.selected?sr(this.selected,this.targetHistory,this.historyLoading,this.saving,this.detailDirty,this.detailTab,this.cluster?.members??[],this.channels,this.secrets,this.targetError,{backdrop:r=>this.dismissOnBackdrop(r),close:()=>this.closeDetailDialog(),update:r=>{this.updateTarget(r)},changed:r=>this.updateDetailDirty(r),redirects:r=>this.toggleMaxRedirects(r),delete:()=>{this.deleteTarget()},selectTab:r=>this.selectDetailTab(r),pause:r=>{this.setPaused(r)}}):h}
      <dialog id="secret-dialog" aria-labelledby="secret-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><div class="title-with-help"><h2 id="secret-title">Add secret</h2>${Y("add-secret-help","About adding a secret","Create an encrypted, write-only value to reference from target requests or webhook headers through the HTTP API.")}</div></div>
        <form @submit=${this.createSecret} @input=${()=>this.error=""}>
          <label>Name<input name="name" placeholder="Webhook token" required autofocus /></label>
          <label>Value<input name="value" type="password" autocomplete="new-password" required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("secret-dialog")}>Cancel</button>${P({label:"Create secret",busy:this.saving,error:this.error})}</div>
        </form>
      </dialog>
      <dialog id="channel-dialog" aria-labelledby="channel-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="channel-title">${this.editingChannel?"Edit channel":"Add channel"}</h2></div>
        <notification-channel-form
          .channel=${this.editingChannel}
          .submitLabel=${this.editingChannel?"Save changes":"Create channel"}
          cancel-label="Cancel"
          @channel-saved=${this.channelSaved}
          @channel-cancel=${()=>{this.editingChannel=void 0,this.closeDialog("channel-dialog")}}
        ></notification-channel-form>
      </dialog>
      <dialog id="token-config-dialog" aria-labelledby="token-config-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><div class="title-with-help"><h2 id="token-config-title">Create join token</h2>${Y("join-token-config-help","About join token settings","Choose how many days the token remains valid and whether it can be reused.")}</div></div>
        <form @submit=${this.createJoinToken} @input=${()=>this.error=""}>
          <label>Expiration (days)<input name="expiration_days" type="number" min="1" step="1" value="1" required autofocus /></label>
          <label class="switch"><span>Unlimited uses</span><input class="switch-control" type="checkbox" role="switch" .checked=${this.unlimitedUses} @change=${r=>this.unlimitedUses=r.target.checked} /></label>
          <label>Maximum uses<input name="max_uses" type="number" min="1" step="1" value="1" ?disabled=${this.unlimitedUses} required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("token-config-dialog")}>Cancel</button>${P({label:this.saving?"Creating...":"Create token",busy:this.saving,error:this.error})}</div>
        </form>
      </dialog>
      <dialog id="join-dialog" aria-labelledby="join-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><div class="title-with-help"><h2 id="join-title">Join token created</h2>${Y("join-token-url-help","About join token credentials","This URL contains cluster credentials. Revoke the token when no longer needed.")}</div></div>
        <div class="join-url">${this.joinUrl}</div>
        <div class="dialog-actions" style="padding: 0 22px 22px"><button class="button secondary" autofocus @click=${()=>this.closeDialog("join-dialog")}>Close</button><button class="button" @click=${this.copyJoinUrl}>${this.copied?"Copied":"Copy URL"}</button></div>
      </dialog>
    `}renderPublicStatusPage(t){const e=t.filter(s=>s.availability==="up"&&!s.paused).length,i=t.filter(s=>s.availability==="down"&&!s.paused).length,a=t.filter(s=>s.paused).length;return c`
      <main class="shell">
        <header>
          ${this.renderBrand()}
          <nav aria-label="Primary"><a class="active" href="/">Status</a></nav>
          <div class="actions">
            <button class="button secondary icon-button" aria-label=${`Theme: ${this.theme}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${Je[this.theme]} aria-hidden="true"></iconify-icon></button>
            <button class="button secondary" type="button" @click=${this.showLogin}>Sign in</button>
          </div>
        </header>
        <section class="heading">
          <div><span class="eyebrow">Public status</span><h1>Status</h1></div>
        </section>
        <section class="summary" aria-label="Target summary">
          <div class="metric"><span>Targets</span><strong>${t.length}</strong></div>
          <div class="metric"><span>Up</span><strong>${e}</strong></div>
          <div class=${`metric down ${i?"active":""}`}><span>Down</span><strong>${i}</strong></div>
          <div class="metric"><span>Paused</span><strong>${a}</strong></div>
        </section>
        ${T({title:"Targets",label:"Public target status",metadata:`${t.length} monitored`,className:"public-status-card",content:c`
            ${t.length?t.map(s=>{const n=s.latest_evaluation,r=s.paused?"paused":s.availability==="down"?"down":s.consecutive_failures>0?"suspicious":s.availability,o=s.paused?"Paused":n?`${n.latency_ms} ms · ${n.status_code??(n.succeeded?"reachable":"unreachable")}`:"Waiting for an evaluation";return c`<div class="resource"><div><strong>${s.name}</strong><code>${s.kind.toUpperCase()} · ${o}</code></div><span class=${`badge ${r}`}>${r}</span></div>`}):c`<upgrid-empty-state>No targets are configured</upgrid-empty-state>`}
          `})}
      </main>${_e()}`}renderOverview(t,e,i,a){const s=this.targets.filter(l=>this.selectedIds.has(l.id)),n=s.some(l=>!l.paused),r=s.some(l=>l.paused),o=this.secrets.filter(l=>!l.referenced);return c`
      <section class="heading" id="overview">
        <div><span class="eyebrow">Cluster status</span><h1>Overview</h1></div>
        <button class="button" @click=${this.openTargetDialog}>Add target</button>
      </section>
      <section class="overview-top">
        <section class="summary" aria-label="Target summary">
          <div class="metric"><span>Targets</span><strong>${this.targets.length}</strong></div>
          <div class="metric"><span>Pending alerts</span><strong>${a}</strong></div>
          <div class="metric"><span>Up</span><strong>${e}</strong></div>
          <div class=${`metric down ${i?"active":""}`}><span>Down</span><strong>${i}</strong></div>
        </section>
        ${T({title:"Secrets",tooltip:{id:"secrets-help",label:"About reusable secrets",message:"Reusable secrets are encrypted and write-only. Reference them from target headers or bodies and webhook headers or other notification channel credentials. UpGrid reports whether each secret is referenced by an active or trashed target or a notification channel."},actions:[...o.length?[{key:"delete-unused",label:`Delete unused (${o.length})`,variant:"danger",disabled:this.saving,onClick:()=>this.cleanupSecrets()}]:[],{key:"add-secret",label:"Add secret",variant:"secondary",onClick:()=>this.showDialog("secret-dialog")}],content:c`
            ${this.secrets.length?this.secrets.map(l=>c`<div class="resource"><div><strong>${l.name}</strong><code>${l.id} · ${l.referenced?"In use":"Unused"}</code></div><button class="button danger icon-button" aria-label=${`Delete secret ${l.name}`} title=${`Delete ${l.name}`} @click=${()=>this.deleteResource("secrets",l.id,l.name)}><iconify-icon .icon=${ne} aria-hidden="true"></iconify-icon></button></div>`):c`<upgrid-empty-state>No reusable secrets</upgrid-empty-state>`}
          `})}
      </section>
      ${T({title:"Targets",metadata:`${this.targets.length} configured`,content:c`
          <div class="toolbar">
            <input aria-label="Search targets" type="search" placeholder="Search name or URL" .value=${this.search} @input=${l=>this.search=l.target.value} />
            <select aria-label="Filter targets" .value=${this.statusFilter} @change=${l=>this.statusFilter=l.target.value}><option value="all">All states</option><option value="up">Up</option><option value="down">Down</option><option value="unknown">Unknown</option><option value="paused">Paused</option></select>
            <select aria-label="Sort targets" .value=${this.sort} @change=${l=>this.sort=l.target.value}><option value="name">Sort by name</option><option value="status">Sort by status</option></select>
          </div>
          ${this.selectedIds.size?c`<div class="bulk"><span class="meta">${this.selectedIds.size} selected</span><div class="bulk-actions"><button class="button secondary icon-button" aria-label="Unselect all" title="Unselect all" @click=${()=>this.selectedIds=new Set}><iconify-icon .icon=${si} aria-hidden="true"></iconify-icon></button>${n?c`<button class="button warning icon-button" aria-label="Pause selected" title="Pause selected" @click=${()=>this.bulkPause(!0)}><iconify-icon .icon=${ti} aria-hidden="true"></iconify-icon></button>`:h}${r?c`<button class="button success icon-button" aria-label="Resume selected" title="Resume selected" @click=${()=>this.bulkPause(!1)}><iconify-icon .icon=${ii} aria-hidden="true"></iconify-icon></button>`:h}<button class="button danger icon-button" aria-label="Delete selected" title="Delete selected" @click=${this.bulkDelete}><iconify-icon .icon=${ne} aria-hidden="true"></iconify-icon></button></div></div>`:h}
          ${t.length?t.map(l=>this.renderTarget(l)):c`<upgrid-empty-state>${this.targets.length?"No targets match these filters":"No targets yet. Add the first one to begin monitoring"}</upgrid-empty-state>`}
        `})}
    `}renderTrashPage(){return c`
      <section class="heading" id="trash">
        <div><span class="eyebrow">Recover deleted monitors</span><h1>Trash</h1></div>
      </section>
      ${T({title:"Deleted targets",label:"Trashed targets",tooltip:{id:"trash-retention-help",label:"About deleted target retention",message:"Settings and history remain recoverable until the retention deadline."},metadata:`${this.trashedTargets.length} stored`,content:c`${this.trashedTargets.length?this.trashedTargets.map(t=>this.renderTrashedTarget(t)):c`<upgrid-empty-state>Trash is empty</upgrid-empty-state>`}`})}
    `}renderTrashedTarget(t){return c`
      <div class="resource">
        <div>
          <strong>${t.name}</strong>
          <code>${t.kind.toUpperCase()} · deleted ${new Date(t.deleted_at_ms).toLocaleString()} · permanently deleted ${new Date(t.purge_at_ms).toLocaleString()}</code>
        </div>
        <div class="actions">
          <button class="button secondary" ?disabled=${this.saving} @click=${()=>this.restoreTarget(t)}>Restore</button>
          <button class="button danger" ?disabled=${this.saving} @click=${()=>this.purgeTarget(t)}>Delete permanently</button>
        </div>
      </div>
    `}renderClusterMember(t){return c`
      <div class="resource">
        <div>
          <strong>${t.name}</strong>
          <code>${t.raft_url} · ${t.active_assignments} active assignments</code>
        </div>
        <div class="actions">
          ${t.local?c`<span class="badge">This node</span>`:h}
          ${t.leader?c`<span class="badge">Leader</span>`:h}
          ${t.draining?c`<span class="badge">Draining</span>`:h}
          ${t.local?h:c`
                <button class="button secondary" ?disabled=${this.saving} @click=${()=>this.setNodeDrain(t,!t.draining)}>${t.draining?"Cancel drain":"Drain"}</button>
                ${t.draining&&t.active_assignments===0?c`<button class="button danger" ?disabled=${this.saving} @click=${()=>this.removeNode(t,!1)}>Remove</button>`:h}
                <button class="button danger" ?disabled=${this.saving} @click=${()=>this.removeNode(t,!0)}>Replace failed</button>
              `}
        </div>
      </div>
    `}renderClusterPage(){return c`
      <section class="heading" id="cluster">
        <div><span class="eyebrow">Raft membership</span><h1>Cluster</h1></div>
        <div class="actions">
          <button class="button" @click=${this.openTokenDialog}>Create token</button>
        </div>
      </section>
      <div class="page-columns">
      ${T({title:"Nodes",label:"Cluster topology",tooltip:{id:"nodes-removal-help",label:"About removing nodes",message:"Drain healthy nodes before removal. Replace failed nodes only after confirming the old process is permanently stopped."},metadata:`${this.cluster?.members.length??0} members`,content:c`
          ${this.cluster?.members.map(t=>this.renderClusterMember(t))}
          ${this.cluster?.members.length?h:c`<upgrid-empty-state>Cluster topology unavailable</upgrid-empty-state>`}
        `})}
      ${T({title:"Join tokens",metadata:`${this.joinTokens.length} stored`,content:c`
          ${this.joinTokens.length?this.joinTokens.map(t=>c`
                    <div class="resource">
                      <div><strong>${t.id.slice(0,12)}…</strong><code>Expires ${new Date(t.expires_at_ms).toLocaleString()} · ${t.remaining_uses===null?"unlimited uses":`${t.remaining_uses} uses left`}</code></div>
                      <button class="button danger" aria-label=${`Revoke join token ${t.id.slice(0,12)}`} @click=${()=>this.revokeJoinToken(t)}>Revoke</button>
                    </div>
                  `):c`<upgrid-empty-state>No join tokens</upgrid-empty-state>`}
        `})}
      </div>
    `}renderTarget(t){const e=t.kind==="node",i=t.kind==="http",a=t.latest_evaluation,s=t.history.slice(0,16).reverse(),n=Math.max(1,...s.map(o=>o.latency_ms)),r=t.paused?"paused":t.availability==="down"?"down":t.consecutive_failures>0?"suspicious":t.availability;return c`
      <div class="target-wrap">
        ${e?c`<input class="select-target" type="checkbox" aria-label=${`Select ${t.name}`} disabled />`:c`<input class="select-target" type="checkbox" aria-label=${`Select ${t.name}`} .checked=${this.selectedIds.has(t.id)} @change=${o=>this.toggleSelected(t.id,o.target.checked)} />`}
        <button class=${`target ${e?"node-target":""}`} aria-label=${t.name} @click=${()=>this.openTarget(t)}>
          <i class="state ${r}" aria-label=${r}></i>
          <div>
            <div class="target-title"><h3>${t.name}</h3><span class="badge">${e?"Node":t.kind.toUpperCase()}</span></div>
            <div class="meta">${t.paused?"Paused · ":""}${i||e?`${t.method} · `:""}${t.url} · every ${t.interval_seconds}s${e?"":` · ${t.locations} ${t.locations===1?"location":"locations"}`}</div>
          </div>
          <div class="target-side">
            ${s.length?c`<div class="mini-chart" aria-hidden="true">${s.map(o=>c`<i class="mini-bar ${o.succeeded?"up":"down"}" style=${`height: ${Math.max(12,o.latency_ms/n*100)}%`}></i>`)}</div>`:h}
            <div class="latency">
              <strong>${a?`${a.latency_ms} ms`:"—"}</strong>
              <span>${a?i?a.status_code??"network error":a.succeeded?"reachable":"unreachable":"waiting"}</span>
            </div>
          </div>
        </button>
      </div>
    `}};st.styles=F`
    :host {
      color-scheme: dark;
      --bg: #090d0c;
      --panel: #111715;
      --panel-2: #151d1a;
      --line: #27322e;
      --muted: #8fa099;
      --text: #edf7f2;
      --green: #58e29c;
      --red: #ff7575;
      --amber: #f2c264;
      --page-background:
        radial-gradient(circle at 12% -5%, #18392d 0, transparent 30%),
        linear-gradient(145deg, #090d0c 0%, #0c1210 55%, #09100d 100%);
      --brand-shadow: #40d89035;
      --nav-bg: #0d1210aa;
      --active-bg: #202b27;
      --button-border: #3e765a;
      --button-bg: #1c4a35;
      --button-text: #e8fff2;
      --button-hover-border: #62b988;
      --disabled-border: #56615c;
      --disabled-bg: #343d39;
      --disabled-text: #9ca6a1;
      --panel-surface: #111715dc;
      --panel-shadow: #0002;
      --divider: #202925;
      --badge-border: #3c554a;
      --badge-text: #a7c3b7;
      --row-hover: #17201c;
      --notice-border: #7b3937;
      --notice-bg: #391b1a;
      --notice-text: #ffb3af;
      --bulk-bg: #16221d;
      --dialog-shadow: #000b;
      --backdrop: #040706cc;
      --input-bg: #0c110f;
      --focus: #4b936c;
      --danger-text: #ff9b97;
      --danger-border: #633b39;
      --warning-bg: #594315;
      --warning-text: #ffd778;
      --warning-border: #9c7625;
      --join-bg: #0b110e;
      display: flex; flex-direction: column;
      min-height: 100vh;
      background: var(--page-background);
      color: var(--text);
      font: 14px/1.5 Inter, ui-sans-serif, system-ui, sans-serif;
      transition: background 220ms ease, color 180ms ease;
    }
    * { box-sizing: border-box; }
    button, input, select { font: inherit; }
    .shell { flex: 1 0 auto; width: 100%; max-width: 1200px; margin: auto; padding: 28px 24px 48px; }
    .setup-shell { display: grid; grid-template-rows: auto minmax(0, 1fr); padding-top: 20px; padding-bottom: 20px; }
    .setup-shell header { margin-bottom: 18px; } .setup-shell upgrid-setup { align-self: center; }
    header { display: grid; grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr); align-items: center; margin-bottom: 34px; }
    .brand, .actions, nav { display: flex; align-items: center; }
    header > .brand { justify-self: start; }
    header > nav { justify-self: center; }
    header > .actions { justify-self: end; }
    .brand img { width: 42px; height: 42px; filter: drop-shadow(0 0 18px var(--brand-shadow)); }
    .brand { gap: 10px; }
    .brand-link { display: grid; place-items: center; border-radius: 10px; }
    .brand-link img { display: block; }
    .live { display: inline-flex; align-items: center; gap: 6px; color: var(--muted); font-size: 12px; }
    .status-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--red); box-shadow: 0 0 0 3px color-mix(in srgb, var(--red) 18%, transparent); }
    .status-dot.online { background: var(--green); box-shadow: 0 0 0 3px color-mix(in srgb, var(--green) 18%, transparent); }
    .eyebrow, .meta { color: var(--muted); font-size: 12px; }
    nav { gap: 4px; padding: 4px; border: 1px solid var(--line); border-radius: 11px; background: var(--nav-bg); }
    nav a { color: var(--muted); padding: 7px 11px; text-decoration: none; border-radius: 7px; transition: background-color 160ms ease, color 160ms ease; }
    nav a.active { color: var(--text); background: var(--active-bg); }
    .actions { gap: 12px; }
    .heading { display: flex; align-items: flex-end; justify-content: space-between; margin-bottom: 30px; }
    .heading h1 { margin: 2px 0 0; font-size: clamp(27px, 4vw, 38px); line-height: 1.1; letter-spacing: -.035em; }
    .eyebrow { text-transform: uppercase; letter-spacing: .16em; }
    .button { min-height: 44px; border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; white-space: nowrap; cursor: pointer; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    .button:hover { border-color: var(--button-hover-border); }
    .button:active { transform: translateY(1px); }
    .button:disabled { border-color: var(--disabled-border); background: var(--disabled-bg); color: var(--disabled-text); cursor: not-allowed; opacity: 1; }
    .button[aria-busy="true"] { cursor: wait; }
    .icon-button { display: grid; width: 44px; height: 44px; min-height: 44px; place-items: center; padding: 0; }
    iconify-icon { display: inline-block; width: 18px; height: 18px; font-size: 18px; }
    .account-menu { position: relative; }
    .account-menu summary { list-style: none; }
    .account-dropdown { position: absolute; top: calc(100% + 8px); right: 0; z-index: 20; display: grid; width: max-content; min-width: 180px; gap: 2px; border: 1px solid var(--line); border-radius: 14px; background: var(--panel); padding: 6px; box-shadow: 0 16px 40px var(--dialog-shadow); }
    .account-dropdown .button { display: flex; width: 100%; min-height: 44px; align-items: center; justify-content: flex-start; box-sizing: border-box; border: 0; border-radius: 10px; background: transparent; padding: 9px 13px; color: var(--muted); font: inherit; line-height: 1.2; text-align: left; text-decoration: none; }
    .account-dropdown .button:hover, .account-dropdown .button:focus-visible { background: var(--row-hover); color: var(--text); }
    .account-separator { height: 1px; margin: 4px 0; background: var(--divider); }
    .account-dropdown .danger { color: var(--danger-text); }
    .account-dropdown .danger:hover, .account-dropdown .danger:focus-visible { background: var(--notice-bg); color: var(--danger-text); }
    ${ht}
    ${_i}
    .auth-panel { width: min(440px, 100%); margin: auto; }
    .admin-page { width: min(760px, 100%); margin: auto; }
    .change-password-page { width: min(440px, 100%); }
    .change-password-page .auth-panel { width: 100%; }
    .token-value { margin: 14px; overflow-wrap: anywhere; }
    .token-value code { display: block; margin: 8px 0; }
    .overview-top { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; margin-bottom: 18px; }
    .public-status-card { margin-top: 18px; }
    .page-columns { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; }
    .summary { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .metric { border: 1px solid var(--line); background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); transition: background-color 180ms ease, border-color 180ms ease, box-shadow 180ms ease; }
    .metric { border-radius: 14px; padding: 17px 18px; }
    .metric span { display: block; color: var(--muted); font-size: 11px; letter-spacing: .11em; text-transform: uppercase; }
    .metric strong { display: block; margin-top: 5px; font-size: 29px; font-weight: 560; }
    .metric.down.active span, .metric.down.active strong { color: var(--red); }
    .resource { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 13px 20px; border-bottom: 1px solid var(--divider); }
    .resource:last-child { border-bottom: 0; }
    .resource strong { display: block; font-size: 13px; }
    .resource code { color: var(--muted); font-size: 11px; }
    .user-resource { padding: 0 20px 0 0; transition: background-color 150ms ease; }
    .user-resource:hover { background: var(--row-hover); }
    .resource-main { display: block; min-width: 0; flex: 1; border: 0; background: transparent; color: var(--text); padding: 13px 20px; text-align: left; }
    .badge { border: 1px solid var(--badge-border); border-radius: 999px; color: var(--badge-text); padding: 2px 7px; font-size: 10px; text-transform: uppercase; }
    .badge.up { border-color: var(--green); color: var(--green); }
    .badge.down { border-color: var(--red); color: var(--red); }
    .transition-main { display: flex; align-items: center; gap: 12px; }
    .channel-resource { display: grid; grid-template-columns: minmax(0, 1fr) auto; }
    .channel-summary { min-width: 0; }
    .channel-title, .channel-actions { display: flex; align-items: center; gap: 10px; }
    .channel-summary code { display: block; margin-top: 2px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .channel-actions .switch span { font-size: 12px; }
    .alert-history { margin-bottom: 20px; }
    .alert-filters { display: grid; grid-template-columns: minmax(180px, 1fr) repeat(3, minmax(120px, auto)); gap: 10px; padding: 14px 20px; border-bottom: 1px solid var(--line); }
    .alert-filters label { display: grid; gap: 5px; color: var(--muted); font-size: 11px; }
    .alert-resource { display: grid; grid-template-columns: minmax(0, 1fr) auto; }
    .alert-summary { display: grid; min-width: 0; gap: 4px; }
    .alert-summary code, .alert-summary .meta { font-size: 11px; }
    .alert-summary .meta { color: var(--muted); white-space: normal; }
    .alert-actions { display: flex; align-items: center; gap: 8px; }
    .target-wrap { display: grid; grid-template-columns: auto minmax(0, 1fr); align-items: center; border-bottom: 1px solid var(--divider); padding-left: 20px; }
    .target-wrap:last-child { border-bottom: 0; }
    .select-target { width: 18px; min-height: 18px; height: 18px; margin: 0; accent-color: var(--button-bg); cursor: pointer; }
    .target { width: 100%; display: grid; grid-template-columns: auto minmax(0, 1fr) auto; gap: 14px; align-items: center; padding: 17px 20px 17px 14px; border: 0; background: transparent; color: var(--text); text-align: left; cursor: pointer; }
    .target-wrap, .target { transition: background-color 150ms ease; }
    .target-wrap:hover, .target-wrap:hover .target { background: var(--row-hover); }
    .node-target { cursor: pointer; }
    .state { width: 10px; height: 10px; border-radius: 50%; color: var(--amber); background: var(--amber); box-shadow: 0 0 12px currentColor; transition: background-color 160ms ease, color 160ms ease, box-shadow 160ms ease; }
    .state.up { color: var(--green); background: var(--green); }
    .state.down { color: var(--red); background: var(--red); }
    .state.paused { color: var(--muted); background: var(--muted); box-shadow: none; }
    .target h3 { margin: 0 0 3px; font-size: 14px; }
    .target-title { display: flex; align-items: center; gap: 8px; margin-bottom: 3px; }
    .target-title h3 { margin: 0; }
    .meta { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .latency { text-align: right; }
    .latency strong { display: block; font-weight: 500; }
    .latency span { color: var(--muted); font-size: 11px; }
    .target-side { display: flex; align-items: center; gap: 20px; }
    .mini-chart { display: flex; width: 88px; height: 32px; align-items: flex-end; gap: 2px; }
    .mini-bar { flex: 1; min-width: 2px; max-width: 7px; border-radius: 2px 2px 1px 1px; opacity: .75; transition: background-color 160ms ease, height 180ms ease, opacity 160ms ease; }
    .mini-bar.up { background: var(--green); }
    .mini-bar.down { background: var(--red); }
    .notice { margin: 0 0 16px; border: 1px solid var(--notice-border); border-radius: 14px; background: var(--notice-bg); color: var(--notice-text); padding: 10px 12px; }
    .toolbar { display: grid; grid-template-columns: minmax(180px, 1fr) auto auto; gap: 8px; padding: 12px 20px; border-bottom: 1px solid var(--line); }
    .toolbar input, .toolbar select { padding: 7px 9px; }
    .toolbar select { appearance: none; padding-right: 38px; background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Cpath d='m6 9 6 6 6-6' fill='none' stroke='%235f7168' stroke-linecap='round' stroke-linejoin='round' stroke-width='2'/%3E%3C/svg%3E"); background-position: right 14px center; background-repeat: no-repeat; background-size: 16px; }
    .bulk { display: flex; align-items: center; gap: 8px; padding: 10px 20px; border-bottom: 1px solid var(--line); background: var(--bulk-bg); }
    .bulk-actions { display: flex; align-items: center; gap: 8px; margin-left: auto; }
    .bulk, .bulk-actions .button { animation: reveal 160ms ease-out; }
    @keyframes reveal { from { opacity: 0; transform: translateY(-3px); } }
    dialog { width: min(580px, calc(100% - 28px)); max-height: calc(100dvh - 28px); overflow-y: auto; border: 1px solid var(--line); border-radius: 17px; background: var(--panel); color: var(--text); padding: 0; box-shadow: 0 28px 90px var(--dialog-shadow); opacity: 0; transform: translateY(8px) scale(.985); transition: opacity 170ms ease, transform 170ms ease, overlay 170ms allow-discrete, display 170ms allow-discrete; }
    #target-dialog { width: min(720px, calc(100% - 28px)); }
    dialog[open] { opacity: 1; transform: translateY(0) scale(1); }
    dialog::backdrop { background: var(--backdrop); backdrop-filter: blur(5px); opacity: 0; transition: opacity 170ms ease, overlay 170ms allow-discrete, display 170ms allow-discrete; }
    dialog[open]::backdrop { opacity: 1; }
    @starting-style {
      dialog[open] { opacity: 0; transform: translateY(8px) scale(.985); }
      dialog[open]::backdrop { opacity: 0; }
    }
    .dialog-head { position: relative; padding: 20px 58px 15px 22px; border-bottom: 1px solid var(--line); }
    .dialog-head h2 { margin: 0; font-size: 18px; }
    .dialog-head p { margin: 4px 0 0; color: var(--muted); }
    .target-dialog-head { display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 12px 12px 12px 22px; }
    .target-dialog-head h2 { flex: none; }
    .detail-dialog-head { padding-right: 68px; }
    form { display: grid; gap: 13px; padding: 20px 22px 22px; }
    .form-tabs { display: flex; width: fit-content; min-width: 0; max-width: 100%; gap: 4px; border: 1px solid var(--line); border-radius: 14px; background: var(--nav-bg); padding: 4px; overflow-x: auto; }
    .form-tabs button { min-height: 34px; border: 0; border-radius: 10px; background: transparent; color: var(--muted); padding: 7px 11px; white-space: nowrap; cursor: pointer; transition: background-color 160ms ease, color 160ms ease; }
    .form-tabs button:hover { background: transparent; color: var(--muted); }
    .form-tabs button[aria-selected="true"], .form-tabs button[aria-selected="true"]:hover { background: var(--active-bg); color: var(--text); }
    .form-tabs button:disabled { border-color: var(--disabled-border); background: var(--disabled-bg); color: var(--disabled-text); cursor: not-allowed; opacity: 1; }
    .target-tab-panel { display: grid; gap: 13px; min-height: 190px; align-content: start; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    .endpoint-row { grid-template-columns: minmax(140px, 1fr) minmax(0, 2fr); }
    label { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
    [hidden] { display: none !important; }
    input, select { width: 100%; min-height: 44px; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; font-size: 16px; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease; }
    input:focus, select:focus { border-color: var(--focus); }
    button:focus-visible, a:focus-visible, .target:focus-visible, input:focus-visible, select:focus-visible { outline: 2px solid var(--green); outline-offset: 2px; }
    button, a, summary, [role="button"], [role="tab"], input[type="checkbox"], input[type="radio"], select, .target, .switch, .checkbox-option { cursor: pointer; user-select: none; }
    button:disabled { border-color: var(--disabled-border); background: var(--disabled-bg); color: var(--disabled-text); cursor: not-allowed; opacity: 1; }
    input:disabled, select:disabled { cursor: not-allowed; }
    input:disabled { cursor: not-allowed; opacity: .5; }
    .dialog-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 5px; }
    .danger-actions { display: flex; gap: 8px; margin-right: auto; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .danger { background: transparent; color: var(--danger-text); border-color: var(--danger-border); }
    .danger:hover:not(:disabled) { border-color: var(--danger-text); }
    .warning { background: transparent; color: var(--warning-text); border-color: var(--warning-border); }
    .warning:hover { border-color: var(--warning-text); }
    .success { background: transparent; color: var(--green); border-color: var(--green); }
    .success:hover { border-color: var(--button-text); }
    .dialog-close { position: absolute; top: 12px; right: 14px; border-radius: 14px; }
    .switch { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .setting-copy { display: grid; gap: 3px; color: var(--text); }
    .setting-copy small { color: var(--muted); font-size: 12px; font-weight: 400; }
    .channel-fields, .tls-fields { display: grid; gap: 10px; margin: 8px 0 0; border: 0; padding: 0; }
    .tls-fields legend { display: flex; width: 100%; align-items: center; gap: 12px; margin: 0 0 4px; padding: 0; color: var(--text); font-size: 14px; font-weight: 400; text-align: center; }
    .tls-fields legend::before, .tls-fields legend::after { height: 1px; flex: 1; background: var(--line); content: ""; }
    .tls-fields .meta { white-space: normal; }
    form .badge { font-size: 12px; }
    .channel-options { display: grid; gap: 6px; }
    .channel-options .checkbox-option { min-height: 36px; border-radius: 8px; padding: 5px 8px; background: var(--panel-2); }
    .switch-label { display: flex; min-width: 0; align-items: center; gap: 8px; }
    .switch-label .badge { margin-left: 0; }
    .checkbox-option { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .checkbox-control { width: 18px; min-height: 18px; height: 18px; flex: none; accent-color: var(--button-bg); cursor: pointer; }
    .switch-control { width: 42px; min-height: 24px; height: 24px; flex: none; appearance: none; border-radius: 999px; background: var(--input-bg); padding: 2px; cursor: pointer; }
    .switch-control::after { display: block; width: 16px; height: 16px; border-radius: 50%; background: var(--muted); content: ""; transition: background-color 160ms ease, transform 160ms ease; }
    .switch-control:checked { border-color: var(--button-border); background: var(--button-bg); }
    .switch-control:checked::after { background: var(--button-text); transform: translateX(18px); }
    footer { display: flex; flex: 0 0 auto; width: calc(100% - 48px); max-width: 1152px; flex-direction: column; align-items: center; justify-content: center; gap: 8px; margin: 0 auto; border-top: 1px solid var(--line); padding: 20px 0 24px; color: var(--muted); font-size: 12px; }
    .footer-links, .footer-powered { display: flex; align-items: center; justify-content: center; flex-wrap: wrap; gap: 10px; text-align: center; }
    footer a { display: inline-flex; align-items: center; gap: 4px; border-radius: 4px; color: var(--muted); text-decoration: underline; text-decoration-thickness: 1px; text-underline-offset: 3px; transition: color 160ms ease; }
    footer a:hover { color: var(--text); }
    footer iconify-icon { width: 14px; height: 14px; font-size: 14px; }
    .history { margin: 0 22px 22px; border-top: 1px solid var(--line); padding-top: 18px; }
    .detail-form .details-panel { gap: 0; }
    .detail-form .history { margin: 0; }
    .detail-form .history:first-child { border-top: 0; padding-top: 0; }
    .detail-form .history + .history { margin-top: 18px; }
    .history-head, .chart-legend, .chart-legend span, .chart-axis { display: flex; align-items: center; }
    .history-head { justify-content: space-between; margin-bottom: 12px; }
    .history-head h3 { margin: 0; font-size: 14px; }
    .history-summary { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 8px; }
    .history-summary div { padding: 12px; border: 1px solid var(--line); border-radius: 9px; background: var(--input-bg); }
    .history-summary span { display: block; color: var(--muted); font-size: 10px; letter-spacing: .08em; text-transform: uppercase; }
    .history-summary strong { display: block; margin-top: 5px; font-size: 18px; font-weight: 560; }
    .chart-plot { display: grid; grid-template-columns: 38px minmax(0, 1fr); gap: 7px; }
    .chart-scale { display: flex; height: 140px; flex-direction: column; justify-content: space-between; padding: 1px 0 7px; color: var(--muted); font-size: 9px; text-align: right; }
    .history-chart { display: flex; height: 140px; align-items: flex-end; gap: 3px; padding: 14px 10px 8px; border: 1px solid var(--line); border-radius: 10px; background: var(--input-bg); }
    .history-bar { flex: 1; min-width: 3px; max-width: 16px; border-radius: 3px 3px 1px 1px; opacity: .82; transform-origin: bottom; transition: opacity 120ms ease, transform 120ms ease; }
    .history-bar:hover { opacity: 1; transform: scaleX(1.15); }
    .history-bar.up { background: var(--green); }
    .history-bar.down { background: var(--red); }
    .chart-axis { justify-content: space-between; margin: 5px 0 0 45px; color: var(--muted); font-size: 10px; }
    .chart-legend { justify-content: flex-end; gap: 12px; margin-top: 9px; color: var(--muted); font-size: 10px; }
    .chart-legend span { gap: 5px; }
    .chart-legend i { width: 7px; height: 7px; border-radius: 2px; }
    .chart-legend .up { background: var(--green); }
    .chart-legend .down { background: var(--red); }
    .join-url { margin: 20px 22px; border: 1px solid var(--line); border-radius: 10px; background: var(--join-bg); color: var(--green); padding: 13px; overflow-wrap: anywhere; font: 12px/1.6 ui-monospace, SFMono-Regular, monospace; }
    :host([data-theme="bright"]) {
        color-scheme: light;
        --bg: #f4f8f6;
        --panel: #ffffff;
        --panel-2: #eef5f1;
        --line: #d3dfd9;
        --muted: #5d6e66;
        --text: #16211c;
        --green: #087a49;
        --red: #c53434;
        --amber: #9a6700;
        --page-background:
          radial-gradient(circle at 12% -5%, #d9f2e4 0, transparent 32%),
          linear-gradient(145deg, #fbfdfc 0%, #f3f8f5 55%, #edf5f1 100%);
        --brand-shadow: #159e5240;
        --nav-bg: #ffffffcc;
        --active-bg: #e4efe9;
        --button-border: #16764b;
        --button-bg: #087a49;
        --button-text: #ffffff;
        --button-hover-border: #075f3a;
        --disabled-border: #c2c9c5;
        --disabled-bg: #e3e7e5;
        --disabled-text: #78817d;
        --panel-surface: #ffffffeb;
        --panel-shadow: #2745381a;
        --divider: #e3ebe7;
        --badge-border: #a6beb2;
        --badge-text: #426356;
        --row-hover: #e9f4ee;
        --notice-border: #e2aaa6;
        --notice-bg: #fff0ef;
        --notice-text: #9f2922;
        --bulk-bg: #e8f4ed;
        --dialog-shadow: #233b3050;
        --backdrop: #17251f66;
        --input-bg: #ffffff;
        --focus: #168655;
        --danger-text: #b42318;
        --danger-border: #dda29d;
        --warning-bg: #fff1bd;
        --warning-text: #805b00;
        --warning-border: #d4aa36;
        --join-bg: #eef8f2;
    }
    @media (prefers-reduced-motion: reduce) {
      :host, nav a, .button, .metric, .target-wrap, .target, .state, .mini-bar, .history-bar, dialog, dialog::backdrop, input, select, .help-tooltip-trigger { transition-duration: 0s; }
      .bulk, .bulk-actions .button { animation-duration: 0s; }
    }
    @media (max-width: 720px) {
      .shell { padding: 20px 14px 60px; }
      header { grid-template-columns: minmax(0, 1fr) auto; row-gap: 16px; }
      header > nav { display: flex; grid-column: 1 / -1; grid-row: 2; justify-self: center; }
      .overview-top { grid-template-columns: 1fr; }
      .page-columns { grid-template-columns: 1fr; }
      .toolbar { grid-template-columns: 1fr 1fr; }
      .toolbar input { grid-column: 1 / -1; }
      .heading { align-items: flex-start; gap: 16px; }
      .target-wrap { align-items: start; padding-left: 14px; } .select-target { align-self: start; margin-top: 6px; } .target { grid-template-columns: auto minmax(0, 1fr); gap: 10px; padding: 12px 14px 12px 10px; }
      .target-side { grid-column: 2; display: grid; grid-template-columns: minmax(88px, 1fr) auto; width: 100%; gap: 18px; margin-top: 4px; } .target > .state { align-self: start; margin-top: 5px; } .mini-chart { width: 100%; max-width: 140px; height: 28px; }
      .latency { min-width: 72px; text-align: right; }
      .channel-resource { grid-template-columns: 1fr; }
      .alert-filters { grid-template-columns: 1fr 1fr; }
      .alert-resource { grid-template-columns: 1fr; }
      .alert-actions { margin-top: 8px; }
      .channel-actions { justify-content: space-between; margin-top: 10px; }
      .target-dialog-head { gap: 6px; padding-left: 14px; }
      .target-dialog-head h2 { font-size: 16px; }
      .form-tabs { gap: 0; padding: 2px; }
      .form-tabs button { min-height: 30px; padding: 5px 2px; font-size: 12px; }
    }
  `;st=rr([ie("upgrid-app")],st);
