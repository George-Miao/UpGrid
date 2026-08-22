(function(){const e=document.createElement("link").relList;if(e&&e.supports&&e.supports("modulepreload"))return;for(const s of document.querySelectorAll('link[rel="modulepreload"]'))a(s);new MutationObserver(s=>{for(const n of s)if(n.type==="childList")for(const r of n.addedNodes)r.tagName==="LINK"&&r.rel==="modulepreload"&&a(r)}).observe(document,{childList:!0,subtree:!0});function i(s){const n={};return s.integrity&&(n.integrity=s.integrity),s.referrerPolicy&&(n.referrerPolicy=s.referrerPolicy),s.crossOrigin==="use-credentials"?n.credentials="include":s.crossOrigin==="anonymous"?n.credentials="omit":n.credentials="same-origin",n}function a(s){if(s.ep)return;s.ep=!0;const n=i(s);fetch(s.href,n)}})();const Ee=globalThis,ot=Ee.ShadowRoot&&(Ee.ShadyCSS===void 0||Ee.ShadyCSS.nativeShadow)&&"adoptedStyleSheets"in Document.prototype&&"replace"in CSSStyleSheet.prototype,lt=Symbol(),yt=new WeakMap;let Xt=class{constructor(e,i,a){if(this._$cssResult$=!0,a!==lt)throw Error("CSSResult is not constructable. Use `unsafeCSS` or `css` instead.");this.cssText=e,this.t=i}get styleSheet(){let e=this.o;const i=this.t;if(ot&&e===void 0){const a=i!==void 0&&i.length===1;a&&(e=yt.get(i)),e===void 0&&((this.o=e=new CSSStyleSheet).replaceSync(this.cssText),a&&yt.set(i,e))}return e}toString(){return this.cssText}};const Ni=t=>new Xt(typeof t=="string"?t:t+"",void 0,lt),R=(t,...e)=>{const i=t.length===1?t[0]:e.reduce((a,s,n)=>a+(r=>{if(r._$cssResult$===!0)return r.cssText;if(typeof r=="number")return r;throw Error("Value passed to 'css' function must be a 'css' function result: "+r+". Use 'unsafeCSS' to pass non-literal values, but take care to ensure page security.")})(s)+t[n+1],t[0]);return new Xt(i,t,lt)},Ui=(t,e)=>{if(ot)t.adoptedStyleSheets=e.map(i=>i instanceof CSSStyleSheet?i:i.styleSheet);else for(const i of e){const a=document.createElement("style"),s=Ee.litNonce;s!==void 0&&a.setAttribute("nonce",s),a.textContent=i.cssText,t.appendChild(a)}},xt=ot?t=>t:t=>t instanceof CSSStyleSheet?(e=>{let i="";for(const a of e.cssRules)i+=a.cssText;return Ni(i)})(t):t;const{is:qi,defineProperty:Fi,getOwnPropertyDescriptor:zi,getOwnPropertyNames:Hi,getOwnPropertySymbols:Bi,getPrototypeOf:Vi}=Object,Ue=globalThis,$t=Ue.trustedTypes,Ji=$t?$t.emptyScript:"",Gi=Ue.reactiveElementPolyfillSupport,me=(t,e)=>t,je={toAttribute(t,e){switch(e){case Boolean:t=t?Ji:null;break;case Object:case Array:t=t==null?t:JSON.stringify(t)}return t},fromAttribute(t,e){let i=t;switch(e){case Boolean:i=t!==null;break;case Number:i=t===null?null:Number(t);break;case Object:case Array:try{i=JSON.parse(t)}catch{i=null}}return i}},dt=(t,e)=>!qi(t,e),wt={attribute:!0,type:String,converter:je,reflect:!1,useDefault:!1,hasChanged:dt};Symbol.metadata??=Symbol("metadata"),Ue.litPropertyMetadata??=new WeakMap;let oe=class extends HTMLElement{static addInitializer(e){this._$Ei(),(this.l??=[]).push(e)}static get observedAttributes(){return this.finalize(),this._$Eh&&[...this._$Eh.keys()]}static createProperty(e,i=wt){if(i.state&&(i.attribute=!1),this._$Ei(),this.prototype.hasOwnProperty(e)&&((i=Object.create(i)).wrapped=!0),this.elementProperties.set(e,i),!i.noAccessor){const a=Symbol(),s=this.getPropertyDescriptor(e,a,i);s!==void 0&&Fi(this.prototype,e,s)}}static getPropertyDescriptor(e,i,a){const{get:s,set:n}=zi(this.prototype,e)??{get(){return this[i]},set(r){this[i]=r}};return{get:s,set(r){const o=s?.call(this);n?.call(this,r),this.requestUpdate(e,o,a)},configurable:!0,enumerable:!0}}static getPropertyOptions(e){return this.elementProperties.get(e)??wt}static _$Ei(){if(this.hasOwnProperty(me("elementProperties")))return;const e=Vi(this);e.finalize(),e.l!==void 0&&(this.l=[...e.l]),this.elementProperties=new Map(e.elementProperties)}static finalize(){if(this.hasOwnProperty(me("finalized")))return;if(this.finalized=!0,this._$Ei(),this.hasOwnProperty(me("properties"))){const i=this.properties,a=[...Hi(i),...Bi(i)];for(const s of a)this.createProperty(s,i[s])}const e=this[Symbol.metadata];if(e!==null){const i=litPropertyMetadata.get(e);if(i!==void 0)for(const[a,s]of i)this.elementProperties.set(a,s)}this._$Eh=new Map;for(const[i,a]of this.elementProperties){const s=this._$Eu(i,a);s!==void 0&&this._$Eh.set(s,i)}this.elementStyles=this.finalizeStyles(this.styles)}static finalizeStyles(e){const i=[];if(Array.isArray(e)){const a=new Set(e.flat(1/0).reverse());for(const s of a)i.unshift(xt(s))}else e!==void 0&&i.push(xt(e));return i}static _$Eu(e,i){const a=i.attribute;return a===!1?void 0:typeof a=="string"?a:typeof e=="string"?e.toLowerCase():void 0}constructor(){super(),this._$Ep=void 0,this.isUpdatePending=!1,this.hasUpdated=!1,this._$Em=null,this._$Ev()}_$Ev(){this._$ES=new Promise(e=>this.enableUpdating=e),this._$AL=new Map,this._$E_(),this.requestUpdate(),this.constructor.l?.forEach(e=>e(this))}addController(e){(this._$EO??=new Set).add(e),this.renderRoot!==void 0&&this.isConnected&&e.hostConnected?.()}removeController(e){this._$EO?.delete(e)}_$E_(){const e=new Map,i=this.constructor.elementProperties;for(const a of i.keys())this.hasOwnProperty(a)&&(e.set(a,this[a]),delete this[a]);e.size>0&&(this._$Ep=e)}createRenderRoot(){const e=this.shadowRoot??this.attachShadow(this.constructor.shadowRootOptions);return Ui(e,this.constructor.elementStyles),e}connectedCallback(){this.renderRoot??=this.createRenderRoot(),this.enableUpdating(!0),this._$EO?.forEach(e=>e.hostConnected?.())}enableUpdating(e){}disconnectedCallback(){this._$EO?.forEach(e=>e.hostDisconnected?.())}attributeChangedCallback(e,i,a){this._$AK(e,a)}_$ET(e,i){const a=this.constructor.elementProperties.get(e),s=this.constructor._$Eu(e,a);if(s!==void 0&&a.reflect===!0){const n=(a.converter?.toAttribute!==void 0?a.converter:je).toAttribute(i,a.type);this._$Em=e,n==null?this.removeAttribute(s):this.setAttribute(s,n),this._$Em=null}}_$AK(e,i){const a=this.constructor,s=a._$Eh.get(e);if(s!==void 0&&this._$Em!==s){const n=a.getPropertyOptions(s),r=typeof n.converter=="function"?{fromAttribute:n.converter}:n.converter?.fromAttribute!==void 0?n.converter:je;this._$Em=s;const o=r.fromAttribute(i,n.type);this[s]=o??this._$Ej?.get(s)??o,this._$Em=null}}requestUpdate(e,i,a,s=!1,n){if(e!==void 0){const r=this.constructor;if(s===!1&&(n=this[e]),a??=r.getPropertyOptions(e),!((a.hasChanged??dt)(n,i)||a.useDefault&&a.reflect&&n===this._$Ej?.get(e)&&!this.hasAttribute(r._$Eu(e,a))))return;this.C(e,i,a)}this.isUpdatePending===!1&&(this._$ES=this._$EP())}C(e,i,{useDefault:a,reflect:s,wrapped:n},r){a&&!(this._$Ej??=new Map).has(e)&&(this._$Ej.set(e,r??i??this[e]),n!==!0||r!==void 0)||(this._$AL.has(e)||(this.hasUpdated||a||(i=void 0),this._$AL.set(e,i)),s===!0&&this._$Em!==e&&(this._$Eq??=new Set).add(e))}async _$EP(){this.isUpdatePending=!0;try{await this._$ES}catch(i){Promise.reject(i)}const e=this.scheduleUpdate();return e!=null&&await e,!this.isUpdatePending}scheduleUpdate(){return this.performUpdate()}performUpdate(){if(!this.isUpdatePending)return;if(!this.hasUpdated){if(this.renderRoot??=this.createRenderRoot(),this._$Ep){for(const[s,n]of this._$Ep)this[s]=n;this._$Ep=void 0}const a=this.constructor.elementProperties;if(a.size>0)for(const[s,n]of a){const{wrapped:r}=n,o=this[s];r!==!0||this._$AL.has(s)||o===void 0||this.C(s,void 0,n,o)}}let e=!1;const i=this._$AL;try{e=this.shouldUpdate(i),e?(this.willUpdate(i),this._$EO?.forEach(a=>a.hostUpdate?.()),this.update(i)):this._$EM()}catch(a){throw e=!1,this._$EM(),a}e&&this._$AE(i)}willUpdate(e){}_$AE(e){this._$EO?.forEach(i=>i.hostUpdated?.()),this.hasUpdated||(this.hasUpdated=!0,this.firstUpdated(e)),this.updated(e)}_$EM(){this._$AL=new Map,this.isUpdatePending=!1}get updateComplete(){return this.getUpdateComplete()}getUpdateComplete(){return this._$ES}shouldUpdate(e){return!0}update(e){this._$Eq&&=this._$Eq.forEach(i=>this._$ET(i,this[i])),this._$EM()}updated(e){}firstUpdated(e){}};oe.elementStyles=[],oe.shadowRootOptions={mode:"open"},oe[me("elementProperties")]=new Map,oe[me("finalized")]=new Map,Gi?.({ReactiveElement:oe}),(Ue.reactiveElementVersions??=[]).push("2.1.2");const ct=globalThis,kt=t=>t,Le=ct.trustedTypes,_t=Le?Le.createPolicy("lit-html",{createHTML:t=>t}):void 0,ei="$lit$",J=`lit$${Math.random().toFixed(9).slice(2)}$`,ti="?"+J,Ki=`<${ti}>`,ee=document,be=()=>ee.createComment(""),ve=t=>t===null||typeof t!="object"&&typeof t!="function",ut=Array.isArray,Wi=t=>ut(t)||typeof t?.[Symbol.iterator]=="function",Be=`[ 	
\f\r]`,ue=/<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g,St=/-->/g,At=/>/g,W=RegExp(`>|${Be}(?:([^\\s"'>=/]+)(${Be}*=${Be}*(?:[^ 	
\f\r"'\`<>=]|("|')|))|$)`,"g"),Tt=/'/g,Ct=/"/g,ii=/^(?:script|style|textarea|title)$/i,Qi=t=>(e,...i)=>({_$litType$:t,strings:e,values:i}),d=Qi(1),te=Symbol.for("lit-noChange"),h=Symbol.for("lit-nothing"),Et=new WeakMap,Z=ee.createTreeWalker(ee,129);function si(t,e){if(!ut(t)||!t.hasOwnProperty("raw"))throw Error("invalid template strings array");return _t!==void 0?_t.createHTML(e):e}const Yi=(t,e)=>{const i=t.length-1,a=[];let s,n=e===2?"<svg>":e===3?"<math>":"",r=ue;for(let o=0;o<i;o++){const l=t[o];let c,p,u=-1,f=0;for(;f<l.length&&(r.lastIndex=f,p=r.exec(l),p!==null);)f=r.lastIndex,r===ue?p[1]==="!--"?r=St:p[1]!==void 0?r=At:p[2]!==void 0?(ii.test(p[2])&&(s=RegExp("</"+p[2],"g")),r=W):p[3]!==void 0&&(r=W):r===W?p[0]===">"?(r=s??ue,u=-1):p[1]===void 0?u=-2:(u=r.lastIndex-p[2].length,c=p[1],r=p[3]===void 0?W:p[3]==='"'?Ct:Tt):r===Ct||r===Tt?r=W:r===St||r===At?r=ue:(r=W,s=void 0);const m=r===W&&t[o+1].startsWith("/>")?" ":"";n+=r===ue?l+Ki:u>=0?(a.push(c),l.slice(0,u)+ei+l.slice(u)+J+m):l+J+(u===-2?o:m)}return[si(t,n+(t[i]||"<?>")+(e===2?"</svg>":e===3?"</math>":"")),a]};class ye{constructor({strings:e,_$litType$:i},a){let s;this.parts=[];let n=0,r=0;const o=e.length-1,l=this.parts,[c,p]=Yi(e,i);if(this.el=ye.createElement(c,a),Z.currentNode=this.el.content,i===2||i===3){const u=this.el.content.firstChild;u.replaceWith(...u.childNodes)}for(;(s=Z.nextNode())!==null&&l.length<o;){if(s.nodeType===1){if(s.hasAttributes())for(const u of s.getAttributeNames())if(u.endsWith(ei)){const f=p[r++],m=s.getAttribute(u).split(J),$=/([.?@])?(.*)/.exec(f);l.push({type:1,index:n,name:$[2],strings:m,ctor:$[1]==="."?Xi:$[1]==="?"?es:$[1]==="@"?ts:qe}),s.removeAttribute(u)}else u.startsWith(J)&&(l.push({type:6,index:n}),s.removeAttribute(u));if(ii.test(s.tagName)){const u=s.textContent.split(J),f=u.length-1;if(f>0){s.textContent=Le?Le.emptyScript:"";for(let m=0;m<f;m++)s.append(u[m],be()),Z.nextNode(),l.push({type:2,index:++n});s.append(u[f],be())}}}else if(s.nodeType===8)if(s.data===ti)l.push({type:2,index:n});else{let u=-1;for(;(u=s.data.indexOf(J,u+1))!==-1;)l.push({type:7,index:n}),u+=J.length-1}n++}}static createElement(e,i){const a=ee.createElement("template");return a.innerHTML=e,a}}function le(t,e,i=t,a){if(e===te)return e;let s=a!==void 0?i._$Co?.[a]:i._$Cl;const n=ve(e)?void 0:e._$litDirective$;return s?.constructor!==n&&(s?._$AO?.(!1),n===void 0?s=void 0:(s=new n(t),s._$AT(t,i,a)),a!==void 0?(i._$Co??=[])[a]=s:i._$Cl=s),s!==void 0&&(e=le(t,s._$AS(t,e.values),s,a)),e}class Zi{constructor(e,i){this._$AV=[],this._$AN=void 0,this._$AD=e,this._$AM=i}get parentNode(){return this._$AM.parentNode}get _$AU(){return this._$AM._$AU}u(e){const{el:{content:i},parts:a}=this._$AD,s=(e?.creationScope??ee).importNode(i,!0);Z.currentNode=s;let n=Z.nextNode(),r=0,o=0,l=a[0];for(;l!==void 0;){if(r===l.index){let c;l.type===2?c=new ce(n,n.nextSibling,this,e):l.type===1?c=new l.ctor(n,l.name,l.strings,this,e):l.type===6&&(c=new is(n,this,e)),this._$AV.push(c),l=a[++o]}r!==l?.index&&(n=Z.nextNode(),r++)}return Z.currentNode=ee,s}p(e){let i=0;for(const a of this._$AV)a!==void 0&&(a.strings!==void 0?(a._$AI(e,a,i),i+=a.strings.length-2):a._$AI(e[i])),i++}}class ce{get _$AU(){return this._$AM?._$AU??this._$Cv}constructor(e,i,a,s){this.type=2,this._$AH=h,this._$AN=void 0,this._$AA=e,this._$AB=i,this._$AM=a,this.options=s,this._$Cv=s?.isConnected??!0}get parentNode(){let e=this._$AA.parentNode;const i=this._$AM;return i!==void 0&&e?.nodeType===11&&(e=i.parentNode),e}get startNode(){return this._$AA}get endNode(){return this._$AB}_$AI(e,i=this){e=le(this,e,i),ve(e)?e===h||e==null||e===""?(this._$AH!==h&&this._$AR(),this._$AH=h):e!==this._$AH&&e!==te&&this._(e):e._$litType$!==void 0?this.$(e):e.nodeType!==void 0?this.T(e):Wi(e)?this.k(e):this._(e)}O(e){return this._$AA.parentNode.insertBefore(e,this._$AB)}T(e){this._$AH!==e&&(this._$AR(),this._$AH=this.O(e))}_(e){this._$AH!==h&&ve(this._$AH)?this._$AA.nextSibling.data=e:this.T(ee.createTextNode(e)),this._$AH=e}$(e){const{values:i,_$litType$:a}=e,s=typeof a=="number"?this._$AC(e):(a.el===void 0&&(a.el=ye.createElement(si(a.h,a.h[0]),this.options)),a);if(this._$AH?._$AD===s)this._$AH.p(i);else{const n=new Zi(s,this),r=n.u(this.options);n.p(i),this.T(r),this._$AH=n}}_$AC(e){let i=Et.get(e.strings);return i===void 0&&Et.set(e.strings,i=new ye(e)),i}k(e){ut(this._$AH)||(this._$AH=[],this._$AR());const i=this._$AH;let a,s=0;for(const n of e)s===i.length?i.push(a=new ce(this.O(be()),this.O(be()),this,this.options)):a=i[s],a._$AI(n),s++;s<i.length&&(this._$AR(a&&a._$AB.nextSibling,s),i.length=s)}_$AR(e=this._$AA.nextSibling,i){for(this._$AP?.(!1,!0,i);e!==this._$AB;){const a=kt(e).nextSibling;kt(e).remove(),e=a}}setConnected(e){this._$AM===void 0&&(this._$Cv=e,this._$AP?.(e))}}class qe{get tagName(){return this.element.tagName}get _$AU(){return this._$AM._$AU}constructor(e,i,a,s,n){this.type=1,this._$AH=h,this._$AN=void 0,this.element=e,this.name=i,this._$AM=s,this.options=n,a.length>2||a[0]!==""||a[1]!==""?(this._$AH=Array(a.length-1).fill(new String),this.strings=a):this._$AH=h}_$AI(e,i=this,a,s){const n=this.strings;let r=!1;if(n===void 0)e=le(this,e,i,0),r=!ve(e)||e!==this._$AH&&e!==te,r&&(this._$AH=e);else{const o=e;let l,c;for(e=n[0],l=0;l<n.length-1;l++)c=le(this,o[a+l],i,l),c===te&&(c=this._$AH[l]),r||=!ve(c)||c!==this._$AH[l],c===h?e=h:e!==h&&(e+=(c??"")+n[l+1]),this._$AH[l]=c}r&&!s&&this.j(e)}j(e){e===h?this.element.removeAttribute(this.name):this.element.setAttribute(this.name,e??"")}}class Xi extends qe{constructor(){super(...arguments),this.type=3}j(e){this.element[this.name]=e===h?void 0:e}}class es extends qe{constructor(){super(...arguments),this.type=4}j(e){this.element.toggleAttribute(this.name,!!e&&e!==h)}}class ts extends qe{constructor(e,i,a,s,n){super(e,i,a,s,n),this.type=5}_$AI(e,i=this){if((e=le(this,e,i,0)??h)===te)return;const a=this._$AH,s=e===h&&a!==h||e.capture!==a.capture||e.once!==a.once||e.passive!==a.passive,n=e!==h&&(a===h||s);s&&this.element.removeEventListener(this.name,this,a),n&&this.element.addEventListener(this.name,this,e),this._$AH=e}handleEvent(e){typeof this._$AH=="function"?this._$AH.call(this.options?.host??this.element,e):this._$AH.handleEvent(e)}}class is{constructor(e,i,a){this.element=e,this.type=6,this._$AN=void 0,this._$AM=i,this.options=a}get _$AU(){return this._$AM._$AU}_$AI(e){le(this,e)}}const ss={I:ce},as=ct.litHtmlPolyfillSupport;as?.(ye,ce),(ct.litHtmlVersions??=[]).push("3.3.3");const rs=(t,e,i)=>{const a=i?.renderBefore??e;let s=a._$litPart$;if(s===void 0){const n=i?.renderBefore??null;a._$litPart$=s=new ce(e.insertBefore(be(),n),n,void 0,i??{})}return s._$AI(t),s};const pt=globalThis;let I=class extends oe{constructor(){super(...arguments),this.renderOptions={host:this},this._$Do=void 0}createRenderRoot(){const e=super.createRenderRoot();return this.renderOptions.renderBefore??=e.firstChild,e}update(e){const i=this.render();this.hasUpdated||(this.renderOptions.isConnected=this.isConnected),super.update(e),this._$Do=rs(i,this.renderRoot,this.renderOptions)}connectedCallback(){super.connectedCallback(),this._$Do?.setConnected(!0)}disconnectedCallback(){super.disconnectedCallback(),this._$Do?.setConnected(!1)}render(){return te}};I._$litElement$=!0,I.finalized=!0,pt.litElementHydrateSupport?.({LitElement:I});const ns=pt.litElementPolyfillSupport;ns?.({LitElement:I});(pt.litElementVersions??=[]).push("4.2.2");const z=t=>(e,i)=>{i!==void 0?i.addInitializer(()=>{customElements.define(t,e)}):customElements.define(t,e)};const os={attribute:!0,type:String,converter:je,reflect:!1,hasChanged:dt},ls=(t=os,e,i)=>{const{kind:a,metadata:s}=i;let n=globalThis.litPropertyMetadata.get(s);if(n===void 0&&globalThis.litPropertyMetadata.set(s,n=new Map),a==="setter"&&((t=Object.create(t)).wrapped=!0),n.set(i.name,t),a==="accessor"){const{name:r}=i;return{set(o){const l=e.get.call(this);e.set.call(this,o),this.requestUpdate(r,l,t,!0,o)},init(o){return o!==void 0&&this.C(r,void 0,t,o),o}}}if(a==="setter"){const{name:r}=i;return function(o){const l=this[r];e.call(this,o),this.requestUpdate(r,l,t,!0,o)}}throw Error("Unsupported decorator location: "+a)};function w(t){return(e,i)=>typeof i=="object"?ls(t,e,i):((a,s,n)=>{const r=s.hasOwnProperty(n);return s.constructor.createProperty(n,a),r?Object.getOwnPropertyDescriptor(s,n):void 0})(t,e,i)}function g(t){return w({...t,state:!0,attribute:!1})}const ai={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 4h4v16H6zm8 0h4v16h-4z"/>'},ri={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m5 3l14 9l-14 9V3z"/>'},de={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 6h18m-2 0v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6m3 0V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2m-6 5v6m4-6v6"/>'},ni={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18 6L6 18M6 6l12 12"/>'},ds={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><path d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></g>'};const oi=Object.freeze({left:0,top:0,width:16,height:16}),Me=Object.freeze({rotate:0,vFlip:!1,hFlip:!1}),we=Object.freeze({...oi,...Me}),Ye=Object.freeze({...we,body:"",hidden:!1}),cs=Object.freeze({width:null,height:null}),li=Object.freeze({...cs,...Me});function us(t,e=0){const i=t.replace(/^-?[0-9.]*/,"");function a(s){for(;s<0;)s+=4;return s%4}if(i===""){const s=parseInt(t);return isNaN(s)?0:a(s)}else if(i!==t){let s=0;switch(i){case"%":s=25;break;case"deg":s=90}if(s){let n=parseFloat(t.slice(0,t.length-i.length));return isNaN(n)?0:(n=n/s,n%1===0?a(n):0)}}return e}const ps=/[\s,]+/;function hs(t,e){e.split(ps).forEach(i=>{switch(i.trim()){case"horizontal":t.hFlip=!0;break;case"vertical":t.vFlip=!0;break}})}const di={...li,preserveAspectRatio:""};function Pt(t){const e={...di},i=(a,s)=>t.getAttribute(a)||s;return e.width=i("width",null),e.height=i("height",null),e.rotate=us(i("rotate","")),hs(e,i("flip","")),e.preserveAspectRatio=i("preserveAspectRatio",i("preserveaspectratio","")),e}function gs(t,e){for(const i in di)if(t[i]!==e[i])return!0;return!1}const ci=/^[a-z0-9]+(-[a-z0-9]+)*$/,ke=(t,e,i,a="")=>{const s=t.split(":");if(t.slice(0,1)==="@"){if(s.length<2||s.length>3)return null;a=s.shift().slice(1)}if(s.length>3||!s.length)return null;if(s.length>1){const o=s.pop(),l=s.pop(),c={provider:s.length>0?s[0]:a,prefix:l,name:o};return e&&!Pe(c)?null:c}const n=s[0],r=n.split("-");if(r.length>1){const o={provider:a,prefix:r.shift(),name:r.join("-")};return e&&!Pe(o)?null:o}if(i&&a===""){const o={provider:a,prefix:"",name:n};return e&&!Pe(o,i)?null:o}return null},Pe=(t,e)=>t?!!((e&&t.prefix===""||t.prefix)&&t.name):!1;function ms(t,e){const i=t.icons,a=t.aliases||Object.create(null),s=Object.create(null);function n(r){if(i[r])return s[r]=[];if(!(r in s)){s[r]=null;const o=a[r]&&a[r].parent,l=o&&n(o);l&&(s[r]=[o].concat(l))}return s[r]}return Object.keys(i).concat(Object.keys(a)).forEach(n),s}function fs(t,e){const i={};!t.hFlip!=!e.hFlip&&(i.hFlip=!0),!t.vFlip!=!e.vFlip&&(i.vFlip=!0);const a=((t.rotate||0)+(e.rotate||0))%4;return a&&(i.rotate=a),i}function Dt(t,e){const i=fs(t,e);for(const a in Ye)a in Me?a in t&&!(a in i)&&(i[a]=Me[a]):a in e?i[a]=e[a]:a in t&&(i[a]=t[a]);return i}function bs(t,e,i){const a=t.icons,s=t.aliases||Object.create(null);let n={};function r(o){n=Dt(a[o]||s[o],n)}return r(e),i.forEach(r),Dt(t,n)}function ui(t,e){const i=[];if(typeof t!="object"||typeof t.icons!="object")return i;t.not_found instanceof Array&&t.not_found.forEach(s=>{e(s,null),i.push(s)});const a=ms(t);for(const s in a){const n=a[s];n&&(e(s,bs(t,s,n)),i.push(s))}return i}const vs={provider:"",aliases:{},not_found:{},...oi};function Ve(t,e){for(const i in e)if(i in t&&typeof t[i]!=typeof e[i])return!1;return!0}function pi(t){if(typeof t!="object"||t===null)return null;const e=t;if(typeof e.prefix!="string"||!t.icons||typeof t.icons!="object"||!Ve(t,vs))return null;const i=e.icons;for(const s in i){const n=i[s];if(!s||typeof n.body!="string"||!Ve(n,Ye))return null}const a=e.aliases||Object.create(null);for(const s in a){const n=a[s],r=n.parent;if(!s||typeof r!="string"||!i[r]&&!a[r]||!Ve(n,Ye))return null}return e}const Re=Object.create(null);function ys(t,e){return{provider:t,prefix:e,icons:Object.create(null),missing:new Set}}function q(t,e){const i=Re[t]||(Re[t]=Object.create(null));return i[e]||(i[e]=ys(t,e))}function hi(t,e){return pi(e)?ui(e,(i,a)=>{a?t.icons[i]=a:t.missing.add(i)}):[]}function xs(t,e,i){try{if(typeof i.body=="string")return t.icons[e]={...i},!0}catch{}return!1}function $s(t,e){let i=[];return(typeof t=="string"?[t]:Object.keys(Re)).forEach(a=>{(typeof a=="string"&&typeof e=="string"?[e]:Object.keys(Re[a]||{})).forEach(s=>{const n=q(a,s);i=i.concat(Object.keys(n.icons).map(r=>(a!==""?"@"+a+":":"")+s+":"+r))})}),i}let xe=!1;function gi(t){return typeof t=="boolean"&&(xe=t),xe}function $e(t){const e=typeof t=="string"?ke(t,!0,xe):t;if(e){const i=q(e.provider,e.prefix),a=e.name;return i.icons[a]||(i.missing.has(a)?null:void 0)}}function mi(t,e){const i=ke(t,!0,xe);if(!i)return!1;const a=q(i.provider,i.prefix);return e?xs(a,i.name,e):(a.missing.add(i.name),!0)}function It(t,e){if(typeof t!="object")return!1;if(typeof e!="string"&&(e=t.provider||""),xe&&!e&&!t.prefix){let a=!1;return pi(t)&&(t.prefix="",ui(t,(s,n)=>{mi(s,n)&&(a=!0)})),a}const i=t.prefix;return Pe({prefix:i,name:"a"})?!!hi(q(e,i),t):!1}function ws(t){return!!$e(t)}function ks(t){const e=$e(t);return e&&{...we,...e}}function fi(t,e){t.forEach(i=>{const a=i.loaderCallbacks;a&&(i.loaderCallbacks=a.filter(s=>s.id!==e))})}function _s(t){t.pendingCallbacksFlag||(t.pendingCallbacksFlag=!0,setTimeout(()=>{t.pendingCallbacksFlag=!1;const e=t.loaderCallbacks?t.loaderCallbacks.slice(0):[];if(!e.length)return;let i=!1;const a=t.provider,s=t.prefix;e.forEach(n=>{const r=n.icons,o=r.pending.length;r.pending=r.pending.filter(l=>{if(l.prefix!==s)return!0;const c=l.name;if(t.icons[c])r.loaded.push({provider:a,prefix:s,name:c});else if(t.missing.has(c))r.missing.push({provider:a,prefix:s,name:c});else return i=!0,!0;return!1}),r.pending.length!==o&&(i||fi([t],n.id),n.callback(r.loaded.slice(0),r.missing.slice(0),r.pending.slice(0),n.abort))})}))}let Ss=0;function As(t,e,i){const a=Ss++,s=fi.bind(null,i,a);if(!e.pending.length)return s;const n={id:a,icons:e,callback:t,abort:s};return i.forEach(r=>{(r.loaderCallbacks||(r.loaderCallbacks=[])).push(n)}),s}function Ts(t){const e={loaded:[],missing:[],pending:[]},i=Object.create(null);t.sort((s,n)=>s.provider!==n.provider?s.provider.localeCompare(n.provider):s.prefix!==n.prefix?s.prefix.localeCompare(n.prefix):s.name.localeCompare(n.name));let a={provider:"",prefix:"",name:""};return t.forEach(s=>{if(a.name===s.name&&a.prefix===s.prefix&&a.provider===s.provider)return;a=s;const n=s.provider,r=s.prefix,o=s.name,l=i[n]||(i[n]=Object.create(null)),c=l[r]||(l[r]=q(n,r));let p;o in c.icons?p=e.loaded:r===""||c.missing.has(o)?p=e.missing:p=e.pending;const u={provider:n,prefix:r,name:o};p.push(u)}),e}const Ze=Object.create(null);function Ot(t,e){Ze[t]=e}function Xe(t){return Ze[t]||Ze[""]}function Cs(t,e=!0,i=!1){const a=[];return t.forEach(s=>{const n=typeof s=="string"?ke(s,e,i):s;n&&a.push(n)}),a}function ht(t){let e;if(typeof t.resources=="string")e=[t.resources];else if(e=t.resources,!(e instanceof Array)||!e.length)return null;return{resources:e,path:t.path||"/",maxURL:t.maxURL||500,rotate:t.rotate||750,timeout:t.timeout||5e3,random:t.random===!0,index:t.index||0,dataAfterTimeout:t.dataAfterTimeout!==!1}}const Fe=Object.create(null),pe=["https://api.simplesvg.com","https://api.unisvg.com"],De=[];for(;pe.length>0;)pe.length===1||Math.random()>.5?De.push(pe.shift()):De.push(pe.pop());Fe[""]=ht({resources:["https://api.iconify.design"].concat(De)});function jt(t,e){const i=ht(e);return i===null?!1:(Fe[t]=i,!0)}function ze(t){return Fe[t]}function Es(){return Object.keys(Fe)}const Ps={resources:[],index:0,timeout:2e3,rotate:750,random:!1,dataAfterTimeout:!1};function Ds(t,e,i,a){const s=t.resources.length,n=t.random?Math.floor(Math.random()*s):t.index;let r;if(t.random){let k=t.resources.slice(0);for(r=[];k.length>1;){const C=Math.floor(Math.random()*k.length);r.push(k[C]),k=k.slice(0,C).concat(k.slice(C+1))}r=r.concat(k)}else r=t.resources.slice(n).concat(t.resources.slice(0,n));const o=Date.now();let l="pending",c=0,p,u=null,f=[],m=[];typeof a=="function"&&m.push(a);function $(){u&&(clearTimeout(u),u=null)}function S(){l==="pending"&&(l="aborted"),$(),f.forEach(k=>{k.status==="pending"&&(k.status="aborted")}),f=[]}function _(k,C){C&&(m=[]),typeof k=="function"&&m.push(k)}function B(){return{startTime:o,payload:e,status:l,queriesSent:c,queriesPending:f.length,subscribe:_,abort:S}}function N(){l="failed",m.forEach(k=>{k(void 0,p)})}function D(){f.forEach(k=>{k.status==="pending"&&(k.status="aborted")}),f=[]}function T(k,C,V){const O=C!=="success";switch(f=f.filter(v=>v!==k),l){case"pending":break;case"failed":if(O||!t.dataAfterTimeout)return;break;default:return}if(C==="abort"){p=V,N();return}if(O){p=V,f.length||(r.length?ne():N());return}if($(),D(),!t.random){const v=t.resources.indexOf(k.resource);v!==-1&&v!==t.index&&(t.index=v)}l="completed",m.forEach(v=>{v(V)})}function ne(){if(l!=="pending")return;$();const k=r.shift();if(k===void 0){if(f.length){u=setTimeout(()=>{$(),l==="pending"&&(D(),N())},t.timeout);return}N();return}const C={status:"pending",resource:k,callback:(V,O)=>{T(C,V,O)}};f.push(C),c++,u=setTimeout(ne,t.rotate),i(k,e,C.callback)}return setTimeout(ne),B}function bi(t){const e={...Ps,...t};let i=[];function a(){i=i.filter(r=>r().status==="pending")}function s(r,o,l){const c=Ds(e,r,o,(p,u)=>{a(),l&&l(p,u)});return i.push(c),c}function n(r){return i.find(o=>r(o))||null}return{query:s,find:n,setIndex:r=>{e.index=r},getIndex:()=>e.index,cleanup:a}}function Lt(){}const Je=Object.create(null);function Is(t){if(!Je[t]){const e=ze(t);if(!e)return;Je[t]={config:e,redundancy:bi(e)}}return Je[t]}function vi(t,e,i){let a,s;if(typeof t=="string"){const n=Xe(t);if(!n)return i(void 0,424),Lt;s=n.send;const r=Is(t);r&&(a=r.redundancy)}else{const n=ht(t);if(n){a=bi(n);const r=Xe(t.resources?t.resources[0]:"");r&&(s=r.send)}}return!a||!s?(i(void 0,424),Lt):a.query(e,s,i)().abort}function Mt(){}function Os(t){t.iconsLoaderFlag||(t.iconsLoaderFlag=!0,setTimeout(()=>{t.iconsLoaderFlag=!1,_s(t)}))}function js(t){const e=[],i=[];return t.forEach(a=>{(a.match(ci)?e:i).push(a)}),{valid:e,invalid:i}}function he(t,e,i){function a(){const s=t.pendingIcons;e.forEach(n=>{s&&s.delete(n),t.icons[n]||t.missing.add(n)})}if(i&&typeof i=="object")try{if(!hi(t,i).length){a();return}}catch(s){console.error(s)}a(),Os(t)}function Rt(t,e){t instanceof Promise?t.then(i=>{e(i)}).catch(()=>{e(null)}):e(t)}function Ls(t,e){t.iconsToLoad?t.iconsToLoad=t.iconsToLoad.concat(e).sort():t.iconsToLoad=e,t.iconsQueueFlag||(t.iconsQueueFlag=!0,setTimeout(()=>{t.iconsQueueFlag=!1;const{provider:i,prefix:a}=t,s=t.iconsToLoad;if(delete t.iconsToLoad,!s||!s.length)return;const n=t.loadIcon;if(t.loadIcons&&(s.length>1||!n)){Rt(t.loadIcons(s,a,i),c=>{he(t,s,c)});return}if(n){s.forEach(c=>{Rt(n(c,a,i),p=>{he(t,[c],p?{prefix:a,icons:{[c]:p}}:null)})});return}const{valid:r,invalid:o}=js(s);if(o.length&&he(t,o,null),!r.length)return;const l=a.match(ci)?Xe(i):null;if(!l){he(t,r,null);return}l.prepare(i,a,r).forEach(c=>{vi(i,c,p=>{he(t,c.icons,p)})})}))}const gt=(t,e)=>{const i=Ts(Cs(t,!0,gi()));if(!i.pending.length){let o=!0;return e&&setTimeout(()=>{o&&e(i.loaded,i.missing,i.pending,Mt)}),()=>{o=!1}}const a=Object.create(null),s=[];let n,r;return i.pending.forEach(o=>{const{provider:l,prefix:c}=o;if(c===r&&l===n)return;n=l,r=c,s.push(q(l,c));const p=a[l]||(a[l]=Object.create(null));p[c]||(p[c]=[])}),i.pending.forEach(o=>{const{provider:l,prefix:c,name:p}=o,u=q(l,c),f=u.pendingIcons||(u.pendingIcons=new Set);f.has(p)||(f.add(p),a[l][c].push(p))}),s.forEach(o=>{const l=a[o.provider][o.prefix];l.length&&Ls(o,l)}),e?As(e,i,s):Mt},Ms=t=>new Promise((e,i)=>{const a=typeof t=="string"?ke(t,!0):t;if(!a){i(t);return}gt([a||t],s=>{if(s.length&&a){const n=$e(a);if(n){e({...we,...n});return}}i(t)})});function Nt(t){try{const e=typeof t=="string"?JSON.parse(t):t;if(typeof e.body=="string")return{...e}}catch{}}function Rs(t,e){if(typeof t=="object")return{data:Nt(t),value:t};if(typeof t!="string")return{value:t};if(t.includes("{")){const n=Nt(t);if(n)return{data:n,value:t}}const i=ke(t,!0,!0);if(!i)return{value:t};const a=$e(i);if(a!==void 0||!i.prefix)return{value:t,name:i,data:a};const s=gt([i],()=>e(t,i,$e(i)));return{value:t,name:i,loading:s}}let yi=!1;try{yi=navigator.vendor.indexOf("Apple")===0}catch{}function Ns(t,e){switch(e){case"svg":case"bg":case"mask":return e}return e!=="style"&&(yi||t.indexOf("<a")===-1)?"svg":t.indexOf("currentColor")===-1?"bg":"mask"}const Us=/(-?[0-9.]*[0-9]+[0-9.]*)/g,qs=/^-?[0-9.]*[0-9]+[0-9.]*$/g;function et(t,e,i){if(e===1)return t;if(i=i||100,typeof t=="number")return Math.ceil(t*e*i)/i;if(typeof t!="string")return t;const a=t.split(Us);if(a===null||!a.length)return t;const s=[];let n=a.shift(),r=qs.test(n);for(;;){if(r){const o=parseFloat(n);isNaN(o)?s.push(n):s.push(Math.ceil(o*e*i)/i)}else s.push(n);if(n=a.shift(),n===void 0)return s.join("");r=!r}}function Fs(t,e="defs"){let i="";const a=t.indexOf("<"+e);for(;a>=0;){const s=t.indexOf(">",a),n=t.indexOf("</"+e);if(s===-1||n===-1)break;const r=t.indexOf(">",n);if(r===-1)break;i+=t.slice(s+1,n).trim(),t=t.slice(0,a).trim()+t.slice(r+1)}return{defs:i,content:t}}function zs(t,e){return t?"<defs>"+t+"</defs>"+e:e}function Hs(t,e,i){const a=Fs(t);return zs(a.defs,e+a.content+i)}const Bs=t=>t==="unset"||t==="undefined"||t==="none";function xi(t,e){const i={...we,...t},a={...li,...e},s={left:i.left,top:i.top,width:i.width,height:i.height};let n=i.body;[i,a].forEach(S=>{const _=[],B=S.hFlip,N=S.vFlip;let D=S.rotate;B?N?D+=2:(_.push("translate("+(s.width+s.left).toString()+" "+(0-s.top).toString()+")"),_.push("scale(-1 1)"),s.top=s.left=0):N&&(_.push("translate("+(0-s.left).toString()+" "+(s.height+s.top).toString()+")"),_.push("scale(1 -1)"),s.top=s.left=0);let T;switch(D<0&&(D-=Math.floor(D/4)*4),D=D%4,D){case 1:T=s.height/2+s.top,_.unshift("rotate(90 "+T.toString()+" "+T.toString()+")");break;case 2:_.unshift("rotate(180 "+(s.width/2+s.left).toString()+" "+(s.height/2+s.top).toString()+")");break;case 3:T=s.width/2+s.left,_.unshift("rotate(-90 "+T.toString()+" "+T.toString()+")");break}D%2===1&&(s.left!==s.top&&(T=s.left,s.left=s.top,s.top=T),s.width!==s.height&&(T=s.width,s.width=s.height,s.height=T)),_.length&&(n=Hs(n,'<g transform="'+_.join(" ")+'">',"</g>"))});const r=a.width,o=a.height,l=s.width,c=s.height;let p,u;r===null?(u=o===null?"1em":o==="auto"?c:o,p=et(u,l/c)):(p=r==="auto"?l:r,u=o===null?et(p,c/l):o==="auto"?c:o);const f={},m=(S,_)=>{Bs(_)||(f[S]=_.toString())};m("width",p),m("height",u);const $=[s.left,s.top,l,c];return f.viewBox=$.join(" "),{attributes:f,viewBox:$,body:n}}function mt(t,e){let i=t.indexOf("xlink:")===-1?"":' xmlns:xlink="http://www.w3.org/1999/xlink"';for(const a in e)i+=" "+a+'="'+e[a]+'"';return'<svg xmlns="http://www.w3.org/2000/svg"'+i+">"+t+"</svg>"}function Vs(t){return t.replace(/"/g,"'").replace(/%/g,"%25").replace(/#/g,"%23").replace(/</g,"%3C").replace(/>/g,"%3E").replace(/\s+/g," ")}function Js(t){return"data:image/svg+xml,"+Vs(t)}function $i(t){return'url("'+Js(t)+'")'}const Gs=()=>{let t;try{if(t=fetch,typeof t=="function")return t}catch{}};let Ne=Gs();function Ks(t){Ne=t}function Ws(){return Ne}function Qs(t,e){const i=ze(t);if(!i)return 0;let a;if(!i.maxURL)a=0;else{let s=0;i.resources.forEach(r=>{s=Math.max(s,r.length)});const n=e+".json?icons=";a=i.maxURL-s-i.path.length-n.length}return a}function Ys(t){return t===404}const Zs=(t,e,i)=>{const a=[],s=Qs(t,e),n="icons";let r={type:n,provider:t,prefix:e,icons:[]},o=0;return i.forEach((l,c)=>{o+=l.length+1,o>=s&&c>0&&(a.push(r),r={type:n,provider:t,prefix:e,icons:[]},o=l.length),r.icons.push(l)}),a.push(r),a};function Xs(t){if(typeof t=="string"){const e=ze(t);if(e)return e.path}return"/"}const ea=(t,e,i)=>{if(!Ne){i("abort",424);return}let a=Xs(e.provider);switch(e.type){case"icons":{const n=e.prefix,r=e.icons.join(","),o=new URLSearchParams({icons:r});a+=n+".json?"+o.toString();break}case"custom":{const n=e.uri;a+=n.slice(0,1)==="/"?n.slice(1):n;break}default:i("abort",400);return}let s=503;Ne(t+a).then(n=>{const r=n.status;if(r!==200){setTimeout(()=>{i(Ys(r)?"abort":"next",r)});return}return s=501,n.json()}).then(n=>{if(typeof n!="object"||n===null){setTimeout(()=>{n===404?i("abort",n):i("next",s)});return}setTimeout(()=>{i("success",n)})}).catch(()=>{i("next",s)})},ta={prepare:Zs,send:ea};function ia(t,e,i){q(i||"",e).loadIcons=t}function sa(t,e,i){q(i||"",e).loadIcon=t}const Ge="data-style";let wi="";function aa(t){wi=t}function Ut(t,e){let i=Array.from(t.childNodes).find(a=>a.hasAttribute&&a.hasAttribute(Ge));i||(i=document.createElement("style"),i.setAttribute(Ge,Ge),t.appendChild(i)),i.textContent=":host{display:inline-block;vertical-align:"+(e?"-0.125em":"0")+"}span,svg{display:block;margin:auto}"+wi}function ki(){Ot("",ta),gi(!0);let t;try{t=window}catch{}if(t){if(t.IconifyPreload!==void 0){const i=t.IconifyPreload,a="Invalid IconifyPreload syntax.";typeof i=="object"&&i!==null&&(i instanceof Array?i:[i]).forEach(s=>{try{(typeof s!="object"||s===null||s instanceof Array||typeof s.icons!="object"||typeof s.prefix!="string"||!It(s))&&console.error(a)}catch{console.error(a)}})}if(t.IconifyProviders!==void 0){const i=t.IconifyProviders;if(typeof i=="object"&&i!==null)for(const a in i){const s="IconifyProviders["+a+"] is invalid.";try{const n=i[a];if(typeof n!="object"||!n||n.resources===void 0)continue;jt(a,n)||console.error(s)}catch{console.error(s)}}}}return{iconLoaded:ws,getIcon:ks,listIcons:$s,addIcon:mi,addCollection:It,calculateSize:et,buildIcon:xi,iconToHTML:mt,svgToURL:$i,loadIcons:gt,loadIcon:Ms,addAPIProvider:jt,setCustomIconLoader:sa,setCustomIconsLoader:ia,appendCustomStyle:aa,_api:{getAPIConfig:ze,setAPIModule:Ot,sendAPIQuery:vi,setFetch:Ks,getFetch:Ws,listAPIProviders:Es}}}const tt={"background-color":"currentColor"},_i={"background-color":"transparent"},qt={image:"var(--svg)",repeat:"no-repeat",size:"100% 100%"},Ft={"-webkit-mask":tt,mask:tt,background:_i};for(const t in Ft){const e=Ft[t];for(const i in qt)e[t+"-"+i]=qt[i]}function zt(t){return t?t+(t.match(/^[-0-9.]+$/)?"px":""):"inherit"}function ra(t,e,i){const a=document.createElement("span");let s=t.body;s.indexOf("<a")!==-1&&(s+="<!-- "+Date.now()+" -->");const n=t.attributes,r=mt(s,{...n,width:e.width+"",height:e.height+""}),o=$i(r),l=a.style,c={"--svg":o,width:zt(n.width),height:zt(n.height),...i?tt:_i};for(const p in c)l.setProperty(p,c[p]);return a}let fe;function na(){try{fe=window.trustedTypes.createPolicy("iconify",{createHTML:t=>t})}catch{fe=null}}function oa(t){return fe===void 0&&na(),fe?fe.createHTML(t):t}function la(t){const e=document.createElement("span"),i=t.attributes;let a="";i.width||(a="width: inherit;"),i.height||(a+="height: inherit;"),a&&(i.style=a);const s=mt(t.body,i);return e.innerHTML=oa(s),e.firstChild}function it(t){return Array.from(t.childNodes).find(e=>{const i=e.tagName&&e.tagName.toUpperCase();return i==="SPAN"||i==="SVG"})}function Ht(t,e){const i=e.icon.data,a=e.customisations,s=xi(i,a);a.preserveAspectRatio&&(s.attributes.preserveAspectRatio=a.preserveAspectRatio);const n=e.renderedMode;let r;n==="svg"?r=la(s):r=ra(s,{...we,...i},n==="mask");const o=it(t);o?r.tagName==="SPAN"&&o.tagName===r.tagName?o.setAttribute("style",r.getAttribute("style")):t.replaceChild(r,o):t.appendChild(r)}function Bt(t,e,i){const a=i&&(i.rendered?i:i.lastRender);return{rendered:!1,inline:e,icon:t,lastRender:a}}function da(t="iconify-icon"){let e,i;try{e=window.customElements,i=window.HTMLElement}catch{return}if(!e||!i)return;const a=e.get(t);if(a)return a;const s=["icon","mode","inline","noobserver","width","height","rotate","flip"],n=class extends i{_shadowRoot;_initialised=!1;_state;_checkQueued=!1;_connected=!1;_observer=null;_visible=!0;constructor(){super();const o=this._shadowRoot=this.attachShadow({mode:"open"}),l=this.hasAttribute("inline");Ut(o,l),this._state=Bt({value:""},l),this._queueCheck()}connectedCallback(){this._connected=!0,this.startObserver()}disconnectedCallback(){this._connected=!1,this.stopObserver()}static get observedAttributes(){return s.slice(0)}attributeChangedCallback(o){switch(o){case"inline":{const l=this.hasAttribute("inline"),c=this._state;l!==c.inline&&(c.inline=l,Ut(this._shadowRoot,l));break}case"noobserver":{this.hasAttribute("noobserver")?this.startObserver():this.stopObserver();break}default:this._queueCheck()}}get icon(){const o=this.getAttribute("icon");if(o&&o.slice(0,1)==="{")try{return JSON.parse(o)}catch{}return o}set icon(o){typeof o=="object"&&(o=JSON.stringify(o)),this.setAttribute("icon",o)}get inline(){return this.hasAttribute("inline")}set inline(o){o?this.setAttribute("inline","true"):this.removeAttribute("inline")}get observer(){return this.hasAttribute("observer")}set observer(o){o?this.setAttribute("observer","true"):this.removeAttribute("observer")}restartAnimation(){const o=this._state;if(o.rendered){const l=this._shadowRoot;if(o.renderedMode==="svg")try{l.lastChild.setCurrentTime(0);return}catch{}Ht(l,o)}}get status(){const o=this._state;return o.rendered?"rendered":o.icon.data===null?"failed":"loading"}_queueCheck(){this._checkQueued||(this._checkQueued=!0,setTimeout(()=>{this._check()}))}_check(){if(!this._checkQueued)return;this._checkQueued=!1;const o=this._state,l=this.getAttribute("icon");if(l!==o.icon.value){this._iconChanged(l);return}if(!o.rendered||!this._visible)return;const c=this.getAttribute("mode"),p=Pt(this);(o.attrMode!==c||gs(o.customisations,p)||!it(this._shadowRoot))&&this._renderIcon(o.icon,p,c)}_iconChanged(o){const l=Rs(o,(c,p,u)=>{const f=this._state;if(f.rendered||this.getAttribute("icon")!==c)return;const m={value:c,name:p,data:u};m.data?this._gotIconData(m):f.icon=m});l.data?this._gotIconData(l):this._state=Bt(l,this._state.inline,this._state)}_forceRender(){if(!this._visible){const o=it(this._shadowRoot);o&&this._shadowRoot.removeChild(o);return}this._queueCheck()}_gotIconData(o){this._checkQueued=!1,this._renderIcon(o,Pt(this),this.getAttribute("mode"))}_renderIcon(o,l,c){const p=Ns(o.data.body,c),u=this._state.inline;Ht(this._shadowRoot,this._state={rendered:!0,icon:o,inline:u,customisations:l,attrMode:c,renderedMode:p})}startObserver(){if(!this._observer&&!this.hasAttribute("noobserver"))try{this._observer=new IntersectionObserver(o=>{const l=o.some(c=>c.isIntersecting);l!==this._visible&&(this._visible=l,this._forceRender())}),this._observer.observe(this)}catch{if(this._observer){try{this._observer.disconnect()}catch{}this._observer=null}}}stopObserver(){this._observer&&(this._observer.disconnect(),this._observer=null,this._visible=!0,this._connected&&this._forceRender())}};s.forEach(o=>{o in n.prototype||Object.defineProperty(n.prototype,o,{get:function(){return this.getAttribute(o)},set:function(l){l!==null?this.setAttribute(o,l):this.removeAttribute(o)}})});const r=ki();for(const o in r)n[o]=n.prototype[o]=r[o];return e.define(t,n),n}const ca=da()||ki(),{iconLoaded:wr,getIcon:kr,listIcons:_r,addIcon:Sr,addCollection:Ar,calculateSize:Tr,buildIcon:Cr,iconToHTML:Er,svgToURL:Pr,loadIcons:Dr,loadIcon:Ir,setCustomIconLoader:Or,setCustomIconsLoader:jr,addAPIProvider:Lr,_api:Mr}=ca;class G extends Error{constructor(e,i){super(i),this.status=e,this.name="ApiRequestError"}}const st="upgrid-session-expired";let Ae;async function Si(t){if(!t.ok){const e=await t.json().catch(()=>({error:t.statusText}));throw new G(t.status,e.error||t.statusText)}return t.status===204?void 0:t.json()}function ua(){return Ae||(Ae=fetch("/api/v1/auth/session").then(t=>Si(t)).then(()=>{}).finally(()=>{Ae=void 0})),Ae}function Vt(){return window.dispatchEvent(new Event(st)),new G(401,"")}async function b(t,e){const i=()=>fetch(t,{...e,headers:{...e?.body?{"content-type":"application/json"}:{},...e?.headers}});let a=await i();if(a.status===401&&!t.startsWith("/api/v1/auth/")){await a.body?.cancel();try{await ua()}catch(s){throw s instanceof G&&s.status===401?Vt():s}if(a=await i(),a.status===401)throw await a.body?.cancel(),Vt()}return Si(a)}const pa={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4m0-4h.01"/></g>'};var ha=Object.defineProperty,ga=Object.getOwnPropertyDescriptor,re=(t,e,i,a)=>{for(var s=a>1?void 0:a?ga(e,i):e,n=t.length-1,r;n>=0;n--)(r=t[n])&&(s=(a?r(e,i,s):r(s))||s);return a&&s&&ha(e,i,s),s};function ma(t){let e=t;for(;e;){if(e instanceof HTMLDialogElement)return e;if(e.parentElement){e=e.parentElement;continue}const i=e.getRootNode();e=i instanceof ShadowRoot?i.host:null}return null}let F=class extends I{constructor(){super(...arguments),this.disabled=!1,this.focusable=!1,this.contained=!1,this.placement="top",this.label="",this.message="",this.reveal=()=>{this.disabled||this.updateComplete.then(()=>{const t=this.renderRoot.querySelector(".popup");t&&(t.matches(":popover-open")||t.showPopover(),this.positionPopup(),window.addEventListener("resize",this.positionPopup),document.addEventListener("scroll",this.positionPopup,!0))})},this.conceal=()=>{queueMicrotask(()=>{this.matches(":hover")||this.matches(":focus-within")||this.hidePopup()})},this.positionPopup=()=>{const t=this.renderRoot.querySelector(".popup");if(!t?.matches(":popover-open"))return;const e=12,i=6,a=this.contained?ma(this)?.getBoundingClientRect():void 0,s=Math.max(e,a?.left??e),n=Math.min(window.innerWidth-e,a?.right??window.innerWidth-e),r=Math.max(e,a?.top??e),o=Math.min(window.innerHeight-e,a?.bottom??window.innerHeight-e);t.style.maxWidth=this.contained?`${Math.max(0,n-s)}px`:"";const l=this.getBoundingClientRect(),c=t.getBoundingClientRect(),p=l.top-c.height-i,u=l.bottom+i,m=this.placement==="bottom"?u+c.height<=o||p<r?u:p:p>=r||u+c.height>o?p:u,$=Math.min(Math.max(s,l.right-c.width),n-c.width),S=Math.max(r,o-c.height);t.style.left=`${$}px`,t.style.top=`${Math.min(Math.max(r,m),S)}px`}}hidePopup(){const t=this.renderRoot.querySelector(".popup");t?.matches(":popover-open")&&t.hidePopover(),window.removeEventListener("resize",this.positionPopup),document.removeEventListener("scroll",this.positionPopup,!0)}connectedCallback(){super.connectedCallback(),this.addEventListener("mouseenter",this.reveal),this.addEventListener("mouseleave",this.conceal),this.addEventListener("focusin",this.reveal),this.addEventListener("focusout",this.conceal)}disconnectedCallback(){this.hidePopup(),this.removeEventListener("mouseenter",this.reveal),this.removeEventListener("mouseleave",this.conceal),this.removeEventListener("focusin",this.reveal),this.removeEventListener("focusout",this.conceal),super.disconnectedCallback()}updated(){this.tabIndex=this.focusable&&!this.disabled?0:-1,this.disabled&&this.hidePopup(),this.label&&!this.disabled?(this.setAttribute("aria-label","Why this action is disabled"),this.setAttribute("aria-description",this.label)):(this.removeAttribute("aria-label"),this.removeAttribute("aria-description"))}render(){return d`<slot name="trigger"></slot><span class="popup" popover="manual" role="tooltip">${this.message||d`<slot></slot>`}</span>`}};F.styles=R`
    :host {
      position: relative;
      display: inline-flex;
      align-items: center;
    }

    .popup {
      box-sizing: border-box;
      position: fixed;
      inset: auto;
      z-index: 30;
      width: min(280px, calc(100dvw - 24px));
      margin: 0;
      border: 1px solid var(--line);
      border-radius: 9px;
      background: var(--panel-2);
      color: var(--text);
      box-shadow: 0 10px 30px var(--dialog-shadow);
      padding: 9px 10px;
      font-size: 12px;
      font-weight: 400;
      line-height: 1.45;
    }
    .popup:not(:popover-open) { display: none; }
  `;re([w({type:Boolean,reflect:!0})],F.prototype,"disabled",2);re([w({type:Boolean})],F.prototype,"focusable",2);re([w({type:Boolean,reflect:!0})],F.prototype,"contained",2);re([w({reflect:!0})],F.prototype,"placement",2);re([w()],F.prototype,"label",2);re([w()],F.prototype,"message",2);F=re([z("upgrid-tooltip")],F);const ft=R`
  .form-field { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
  .title-with-help { position: relative; display: flex; align-items: center; gap: 3px; }
  .help-tooltip-trigger { display: grid; width: 28px; height: 28px; place-items: center; border: 0; border-radius: 7px; background: transparent; color: var(--muted); padding: 0; cursor: pointer; user-select: none; transition: background-color 160ms ease, color 160ms ease; }
  .help-tooltip-trigger:hover { background: var(--panel-2); color: var(--text); }
  .help-tooltip-trigger iconify-icon { width: 16px; height: 16px; font-size: 16px; }
  .help-tooltip-content a { display: inline-block; margin-top: 5px; color: var(--green); font-weight: 600; }
`;function X(t,e,i,a){return d`
    <upgrid-tooltip placement="bottom" contained>
      <button slot="trigger" class="help-tooltip-trigger" type="button" aria-label=${e} aria-describedby=${t}>
        <iconify-icon .icon=${pa} aria-hidden="true"></iconify-icon>
      </button>
      <span class="help-tooltip-content" id=${t}>
        ${i}
        ${a?d`<a href=${a.href} target="_blank" rel="noreferrer">${a.label}</a>`:null}
      </span>
    </upgrid-tooltip>
  `}function fa(t){const e="labels"in t?t.labels:null;return t.getAttribute("aria-label")??e?.item(0)?.textContent?.trim()??void 0}function Ai(t){const e=fa(t);return t.validity.valueMissing&&e?`Please fill out ${e.toLocaleLowerCase()}`:e?`${e}: ${t.validationMessage}`:t.validationMessage}var ba=Object.defineProperty,va=Object.getOwnPropertyDescriptor,H=(t,e,i,a)=>{for(var s=a>1?void 0:a?va(e,i):e,n=t.length-1,r;n>=0;n--)(r=t[n])&&(s=(a?r(e,i,s):r(s))||s);return a&&s&&ba(e,i,s),s};function ya(t){for(let e=0;e<t.elements.length;e+=1){const i=t.elements.item(e);if(i instanceof HTMLElement&&"validity"in i&&!i.validity.valid)return i}}function Jt(t){return JSON.stringify(Array.from(new FormData(t),([e,i])=>[e,typeof i=="string"?i:`${i.name}:${i.size}:${i.lastModified}`]))}let L=class extends I{constructor(){super(...arguments),this.busy=!1,this.blocked=!1,this.trackChanges=!1,this.error="",this.baselineKey="",this.blockedMessage="Form is unavailable",this.message="",this.baseline="",this.form=null,this.button=null,this.formChanged=()=>this.updateState(),this.formReset=()=>queueMicrotask(()=>this.captureBaseline())}firstUpdated(){this.form=this.closest("form"),this.button=this.querySelector('button[type="submit"]'),this.form?.addEventListener("input",this.formChanged),this.form?.addEventListener("change",this.formChanged),this.form?.addEventListener("reset",this.formReset),this.captureBaseline()}disconnectedCallback(){this.form?.removeEventListener("input",this.formChanged),this.form?.removeEventListener("change",this.formChanged),this.form?.removeEventListener("reset",this.formReset),super.disconnectedCallback()}updated(t){t.get("busy")===!0&&!this.busy&&!this.error?queueMicrotask(()=>this.captureBaseline()):t.has("baselineKey")&&t.get("baselineKey")!==void 0&&queueMicrotask(()=>this.captureBaseline()),this.updateState()}captureBaseline(){!this.form||!this.trackChanges||this.changed!==void 0||(this.baseline=Jt(this.form),this.updateState())}updateState(){if(!this.form||!this.button)return;const t=ya(this.form),e=!this.trackChanges||(this.changed??Jt(this.form)!==this.baseline);this.message=this.error.trim()||(this.blocked?this.blockedMessage:"")||(t?Ai(t):"");const i=this.busy||this.message.length>0||!e;this.button.disabled=i,this.toggleAttribute("disabled",i)}render(){return d`
      <upgrid-tooltip .disabled=${!this.message} .focusable=${!!this.message} .label=${this.message} .message=${this.message}>
        <slot name="trigger" slot="trigger"></slot>
      </upgrid-tooltip>
    `}};L.styles=R`
    :host { display: inline-flex; }
    :host([disabled]) { cursor: not-allowed; }
    ::slotted(button[slot="trigger"]:disabled) { pointer-events: none; }
  `;H([w({type:Boolean})],L.prototype,"busy",2);H([w({type:Boolean})],L.prototype,"blocked",2);H([w({attribute:!1})],L.prototype,"changed",2);H([w({type:Boolean})],L.prototype,"trackChanges",2);H([w()],L.prototype,"error",2);H([w({attribute:"baseline-key"})],L.prototype,"baselineKey",2);H([w({attribute:"blocked-message"})],L.prototype,"blockedMessage",2);H([g()],L.prototype,"message",2);L=H([z("upgrid-form-submit")],L);function E({label:t,busy:e=!1,className:i="button",blocked:a=!1,changed:s,error:n="",baselineKey:r="",blockedMessage:o="Form is unavailable",trackChanges:l=!1}){return d`
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
  `}var xa=Object.defineProperty,$a=Object.getOwnPropertyDescriptor,K=(t,e,i,a)=>{for(var s=a>1?void 0:a?$a(e,i):e,n=t.length-1,r;n>=0;n--)(r=t[n])&&(s=(a?r(e,i,s):r(s))||s);return a&&s&&xa(e,i,s),s};let M=class extends I{constructor(){super(...arguments),this.checked=!1,this.disabled=!1,this.name="",this.value="on",this.ariaLabel="",this.compact=!1,this.formDisabled=!1,this.internals=this.attachInternals()}get form(){return this.internals.form}get validity(){return this.internals.validity}get validationMessage(){return this.internals.validationMessage}get willValidate(){return this.internals.willValidate}checkValidity(){return this.internals.checkValidity()}reportValidity(){return this.internals.reportValidity()}updated(){this.initialChecked??=this.checked,this.updateFormValue()}formDisabledCallback(t){this.formDisabled=t,this.updateFormValue()}formResetCallback(){this.checked=this.initialChecked??!1,this.updateFormValue()}formStateRestoreCallback(t){this.checked=t==="checked",this.updateFormValue()}get controlDisabled(){return this.disabled||this.formDisabled}updateFormValue(){this.internals.setFormValue(this.checked&&!this.controlDisabled?this.value:null,this.checked?"checked":"unchecked")}forward(t){t.stopPropagation(),this.checked=t.currentTarget.checked,this.updateFormValue(),this.dispatchEvent(new Event(t.type,{bubbles:!0,composed:!0}))}render(){return d`
      <label class=${this.controlDisabled?"disabled":""}>
        <span class="label"><slot></slot></span>
        <input type="checkbox" role="switch" .checked=${this.checked} ?disabled=${this.controlDisabled} aria-label=${this.ariaLabel||h} @input=${this.forward} @change=${this.forward} />
      </label>
    `}};M.formAssociated=!0;M.styles=R`
    :host { display: block; min-width: 0; color: var(--muted); font-size: 14px; }
    :host([hidden]) { display: none; }
    :host([compact]) { display: inline-block; width: fit-content; max-width: 100%; align-self: start; justify-self: start; }
    label { display: flex; align-items: center; justify-content: space-between; gap: 12px; font: inherit; cursor: pointer; user-select: none; }
    .label { min-width: 0; }
    :host([compact]) label { justify-content: flex-start; }
    input { box-sizing: border-box; display: flex; width: 42px; min-height: 24px; height: 24px; flex: none; align-items: center; margin: 0; appearance: none; border: 1px solid var(--line); border-radius: 999px; outline: 0; background: var(--input-bg); padding: 2px; cursor: pointer; }
    input::after { display: block; width: 16px; height: 16px; border-radius: 50%; background: var(--muted); content: ""; transition: background-color 160ms ease, transform 160ms ease; }
    input:checked { border-color: var(--button-border); background: var(--button-bg); }
    input:checked::after { background: var(--button-text); transform: translateX(18px); }
    input:focus-visible { outline: 2px solid var(--green); outline-offset: 2px; }
    input:disabled, label.disabled { cursor: not-allowed; }
    input:disabled { opacity: .65; }
    @media (prefers-reduced-motion: reduce) { input, input::after { transition-duration: 0s; } }
  `;K([w({type:Boolean})],M.prototype,"checked",2);K([w({type:Boolean,reflect:!0})],M.prototype,"disabled",2);K([w({reflect:!0})],M.prototype,"name",2);K([w({reflect:!0})],M.prototype,"value",2);K([w({attribute:"aria-label"})],M.prototype,"ariaLabel",2);K([w({type:Boolean,reflect:!0})],M.prototype,"compact",2);K([g()],M.prototype,"formDisabled",2);M=K([z("upgrid-toggle-switch")],M);function Gt(t,e,i=!1){if(e==="telegram"){const a=String(t.get("bot_token")??"");return{type:"telegram",name:t.get("name"),bot_token:i&&!a?void 0:a,chat_id:t.get("chat_id"),default:t.get("default")==="on"}}if(e==="smtp"){const a=String(t.get("username")??""),s=String(t.get("password")??"");return{type:"smtp",name:t.get("name"),host:t.get("host"),port:Number(t.get("port")),security:t.get("security"),username:a||void 0,password:s||void 0,from:t.get("from"),to:t.get("to"),default:t.get("default")==="on"}}return{type:"webhook",name:t.get("name"),url:t.get("url"),headers:i?void 0:{},default:t.get("default")==="on"}}function wa(t){return String(t.get("statuses")??"200-299").split(",").map(e=>{const[i,a]=e.trim().split("-"),s=Number(i);return{start:s,end:a===void 0?s:Number(a)}})}function Ie(t,e=[],i=!0,a=String(t.get("kind")??"http"),s=[]){const n=String(t.get("url")),r=a==="http",o=!r||t.get("follow_redirects")==="on",l=r?n:`${a}://${n.replace(/^[a-z][a-z0-9+.-]*:\/\//i,"")}`;return{name:String(t.get("name")),kind:a,url:l,method:String(t.get("method")??"GET"),accepted_statuses:r?wa(t):[{start:200,end:299}],follow_redirects:o,max_redirects:r?o?Number(t.get("max_redirects")??5):0:5,interval_seconds:Number(t.get("interval")),timeout_seconds:Number(t.get("timeout")),failure_threshold:Number(t.get("failures")),locations:Number(t.get("locations")??1),headers:{},body:null,assertions:s,skip_tls_verification:r&&t.get("skip_tls_verification")==="on",tls_ca_secret_id:Ke(t,"tls_ca_secret_id"),tls_client_certificate_secret_id:Ke(t,"tls_client_certificate_secret_id"),tls_client_private_key_secret_id:Ke(t,"tls_client_private_key_secret_id"),notification_channel_ids:e,use_default_channels:i}}function Ke(t,e){return String(t.get(e)??"")||null}var ka=Object.defineProperty,_a=Object.getOwnPropertyDescriptor,j=(t,e,i,a)=>{for(var s=a>1?void 0:a?_a(e,i):e,n=t.length-1,r;n>=0;n--)(r=t[n])&&(s=(a?r(e,i,s):r(s))||s);return a&&s&&ka(e,i,s),s};let P=class extends I{constructor(){super(...arguments),this.defaultChannel=!1,this.submitLabel="Create channel",this.cancelLabel="Cancel",this.disabled=!1,this.kind="webhook",this.isDefault=!1,this.saving=!1,this.testing=!1,this.message="",this.messageIsError=!1}willUpdate(t){t.has("channel")&&(this.kind=this.channel?.kind??"webhook",this.message="",this.messageIsError=!1),(t.has("channel")||t.has("defaultChannel"))&&(this.isDefault=this.channel?.default??this.defaultChannel)}changeKind(t){this.kind=t.target.value,this.message="",this.messageIsError=!1}formChanged(){this.messageIsError&&(this.message="",this.messageIsError=!1)}async save(t){t.preventDefault();const e=t.currentTarget,i=this.channel!==void 0;this.saving=!0,this.message="";try{const a=await b(i?`/api/v1/channels/${this.channel?.id}`:"/api/v1/channels",{method:i?"PUT":"POST",body:JSON.stringify(Gt(new FormData(e),this.kind,i))});e.reset(),this.kind=this.channel?.kind??"webhook",this.dispatchEvent(new CustomEvent("channel-saved",{detail:a,bubbles:!0,composed:!0}))}catch(a){this.showFailure("Save failed",a)}finally{this.saving=!1}}cancel(){this.dispatchEvent(new CustomEvent("channel-cancel",{bubbles:!0,composed:!0}))}async testConnection(t){const e=t.currentTarget.form;if(!e||![...e.querySelectorAll("[data-test-required]")].every(r=>r.reportValidity()))return;const a=this.channel!==void 0,s=Gt(new FormData(e),this.kind,a),n=this.channel?{...s,channel_id:this.channel.id}:s;this.testing=!0,this.message="";try{await b("/api/v1/channels/test",{method:"POST",body:JSON.stringify(n)}),this.message="Test sent",this.messageIsError=!1}catch(r){this.showFailure("Test failed",r)}finally{this.testing=!1}}showFailure(t,e){this.message=`${t}: ${e instanceof Error?e.message:String(e)}`,this.messageIsError=!0}render(){const t=this.disabled||this.saving||this.testing;return d`<form @submit=${this.save} @input=${this.formChanged}>
      <label>Type<select name="type" .value=${this.kind} ?disabled=${this.channel!==void 0||t} @change=${this.changeKind}><option value="webhook">Webhook</option><option value="telegram">Telegram</option><option value="smtp">SMTP email</option></select></label>
      <label>Name<input name="name" placeholder="On-call" .value=${this.channel?.name??""} required /></label>
      ${this.renderFields()}
      <upgrid-toggle-switch name="default" .checked=${this.isDefault} ?disabled=${t} @change=${e=>this.isDefault=e.currentTarget.checked}>Default channel</upgrid-toggle-switch>
      ${this.message?d`<p class=${`channel-test-message${this.messageIsError?" error":""}`} role="status">${this.message}</p>`:h}
      <div class="dialog-actions"><button class="button secondary" type="button" ?disabled=${t} @click=${this.cancel}>${this.cancelLabel}</button><button class="button secondary" type="button" aria-busy=${this.testing} ?disabled=${t} @click=${this.testConnection}>${this.testing?"Sending...":"Send test"}</button>${E({label:this.saving?"Saving...":this.submitLabel,busy:this.saving,blocked:this.disabled||this.testing,error:this.messageIsError?this.message:"",baselineKey:this.channel?.id??"new",blockedMessage:this.testing?"Channel test is in progress":"Channel form is unavailable",trackChanges:this.channel!==void 0})}</div>
    </form>`}renderFields(){return this.kind==="webhook"?d`<label>Webhook URL<input name="url" type="url" placeholder="https://hooks.example.com/upgrid" .value=${this.channel?.destination??""} data-test-required required /></label>`:this.kind==="telegram"?d`
        <label><span class="title-with-help">Bot token ${X("telegram-token-help","About Telegram bot token storage",this.channel?"Get a replacement token from Telegram's @BotFather. Leave this blank to keep the automatically managed secret, or enter the replacement token.":"Get a bot token from Telegram's @BotFather.")}</span><input name="bot_token" type="password" autocomplete="off" placeholder=${this.channel?"Leave blank to keep current token":""} data-test-required ?required=${this.channel===void 0} /></label>
        <label>Chat ID<input name="chat_id" .value=${this.channel?.destination??""} data-test-required required /></label>
      `:d`
      <label>SMTP host<input name="host" placeholder="smtp.example.com" .value=${this.channel?.destination??""} data-test-required required /></label>
      <div class="row">
        <label>Port<input name="port" type="number" min="1" max="65535" .value=${String(this.channel?.port??587)} data-test-required required /></label>
        <label>Security<select name="security" .value=${this.channel?.security??"start_tls"}><option value="start_tls">STARTTLS</option><option value="tls">Implicit TLS</option><option value="none">Plaintext</option></select></label>
      </div>
      <label>Username<input name="username" autocomplete="username" .value=${this.channel?.username??""} /></label>
      <div class="form-field"><div class="title-with-help"><label for="smtp-password">Password</label>${X("smtp-password-help","About SMTP password storage",this.channel?"Leave this blank to keep the automatically managed secret. Clear the username to disable authentication.":"Enter a username and password together to enable authentication. The password is encrypted as an automatically managed secret.")}</div><input id="smtp-password" name="password" type="password" autocomplete="off" placeholder=${this.channel?"Leave blank to keep current password":"Optional"} /></div>
      <label>From<input name="from" placeholder="UpGrid <upgrid@example.com>" .value=${this.channel?.from??""} data-test-required required /></label>
      <label>Recipient<input name="to" placeholder="on-call@example.com" .value=${this.channel?.to??""} data-test-required required /></label>
    `}};P.styles=R`
    ${ft}
    :host { display: block; }
    *, *::before, *::after { box-sizing: border-box; }
    form { display: grid; gap: 13px; padding: 20px 22px 22px; }
    label { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
    input, select { width: 100%; min-height: 44px; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; font: inherit; font-size: 16px; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease; }
    input:focus, select:focus { border-color: var(--focus); }
    button:focus-visible, input:focus-visible, select:focus-visible { outline: 2px solid var(--green); outline-offset: 2px; }
    button, input[type="checkbox"], select { cursor: pointer; user-select: none; }
    button.button:disabled { border-color: var(--disabled-border); background: var(--disabled-bg); color: var(--disabled-text); cursor: not-allowed; opacity: 1; }
    input:disabled, select:disabled { cursor: not-allowed; opacity: .65; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    .button { min-height: 44px; border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; white-space: nowrap; cursor: pointer; font: inherit; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    .button:hover { border-color: var(--button-hover-border); }
    .button:active { transform: translateY(1px); }
    .secondary { border-color: var(--line); background: transparent; color: var(--muted); }
    .form-field { display: grid; gap: 6px; }
    .title-with-help { display: flex; align-items: center; gap: 6px; color: var(--muted); font-size: 14px; }
    .channel-test-message { margin: 5px 0 0; border: 1px solid var(--line); border-radius: 9px; background: var(--panel-2); color: var(--green); padding: 10px 12px; overflow-wrap: anywhere; white-space: normal; }
    .channel-test-message.error { border-color: var(--notice-border); background: var(--notice-bg); color: var(--notice-text); }
    .dialog-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 5px; }
    @media (max-width: 620px) { .row { grid-template-columns: 1fr; } .dialog-actions { flex-wrap: wrap; } }
    @media (prefers-reduced-motion: reduce) { input, select, .button { transition-duration: 0s; } }
  `;j([w({attribute:!1})],P.prototype,"channel",2);j([w({type:Boolean,attribute:"default-channel"})],P.prototype,"defaultChannel",2);j([w({attribute:"submit-label"})],P.prototype,"submitLabel",2);j([w({attribute:"cancel-label"})],P.prototype,"cancelLabel",2);j([w({type:Boolean})],P.prototype,"disabled",2);j([g()],P.prototype,"kind",2);j([g()],P.prototype,"isDefault",2);j([g()],P.prototype,"saving",2);j([g()],P.prototype,"testing",2);j([g()],P.prototype,"message",2);j([g()],P.prototype,"messageIsError",2);P=j([z("upgrid-notification-channel-form")],P);const Sa={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><path d="M22 12h-6l-2 3h-4l-2-3H2"/><path d="M5.45 5.11L2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z"/></g>'};var Aa=Object.getOwnPropertyDescriptor,Ta=(t,e,i,a)=>{for(var s=a>1?void 0:a?Aa(e,i):e,n=t.length-1,r;n>=0;n--)(r=t[n])&&(s=r(s)||s);return s};let at=class extends I{render(){return d`<div class="state"><span class="illustration" aria-hidden="true"><iconify-icon .icon=${Sa}></iconify-icon></span><p><slot></slot></p></div>`}};at.styles=R`
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
  `;at=Ta([z("upgrid-empty-state")],at);const Ca={CHILD:2},Ea=t=>(...e)=>({_$litDirective$:t,values:e});let Pa=class{constructor(e){}get _$AU(){return this._$AM._$AU}_$AT(e,i,a){this._$Ct=e,this._$AM=i,this._$Ci=a}_$AS(e,i){return this.update(e,i)}update(e,i){return this.render(...i)}};const{I:Da}=ss,Kt=t=>t,Wt=()=>document.createComment(""),ge=(t,e,i)=>{const a=t._$AA.parentNode,s=e===void 0?t._$AB:e._$AA;if(i===void 0){const n=a.insertBefore(Wt(),s),r=a.insertBefore(Wt(),s);i=new Da(n,r,t,t.options)}else{const n=i._$AB.nextSibling,r=i._$AM,o=r!==t;if(o){let l;i._$AQ?.(t),i._$AM=t,i._$AP!==void 0&&(l=t._$AU)!==r._$AU&&i._$AP(l)}if(n!==s||o){let l=i._$AA;for(;l!==n;){const c=Kt(l).nextSibling;Kt(a).insertBefore(l,s),l=c}}}return i},Q=(t,e,i=t)=>(t._$AI(e,i),t),Ia={},Oa=(t,e=Ia)=>t._$AH=e,ja=t=>t._$AH,We=t=>{t._$AR(),t._$AA.remove()};const Qt=(t,e,i)=>{const a=new Map;for(let s=e;s<=i;s++)a.set(t[s],s);return a},La=Ea(class extends Pa{constructor(t){if(super(t),t.type!==Ca.CHILD)throw Error("repeat() can only be used in text expressions")}dt(t,e,i){let a;i===void 0?i=e:e!==void 0&&(a=e);const s=[],n=[];let r=0;for(const o of t)s[r]=a?a(o,r):r,n[r]=i(o,r),r++;return{values:n,keys:s}}render(t,e,i){return this.dt(t,e,i).values}update(t,[e,i,a]){const s=ja(t),{values:n,keys:r}=this.dt(e,i,a);if(!Array.isArray(s))return this.ut=r,n;const o=this.ut??=[],l=[];let c,p,u=0,f=s.length-1,m=0,$=n.length-1;for(;u<=f&&m<=$;)if(s[u]===null)u++;else if(s[f]===null)f--;else if(o[u]===r[m])l[m]=Q(s[u],n[m]),u++,m++;else if(o[f]===r[$])l[$]=Q(s[f],n[$]),f--,$--;else if(o[u]===r[$])l[$]=Q(s[u],n[$]),ge(t,l[$+1],s[u]),u++,$--;else if(o[f]===r[m])l[m]=Q(s[f],n[m]),ge(t,s[u],s[f]),f--,m++;else if(c===void 0&&(c=Qt(r,m,$),p=Qt(o,u,f)),c.has(o[u]))if(c.has(o[f])){const S=p.get(r[m]),_=S!==void 0?s[S]:null;if(_===null){const B=ge(t,s[u]);Q(B,n[m]),l[m]=B}else l[m]=Q(_,n[m]),ge(t,s[u],_),s[S]=null;m++}else We(s[f]),f--;else We(s[u]),u++;for(;m<=$;){const S=ge(t,l[$+1]);Q(S,n[m]),l[m++]=S}for(;u<=f;){const S=s[u++];S!==null&&We(S)}return this.ut=r,Oa(t,l),te}}),Ti=R`
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
`;function Ma(t){const e=t.variant&&t.variant!=="primary"?`button ${t.variant}`:"button";return d`
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
  `}function A({title:t,label:e,tooltip:i,metadata:a,actions:s=[],content:n,footer:r,className:o}){const l=e??t,c=o?`panel ${o}`:"panel";return d`
    <section class=${c} aria-label=${l??h}>
      ${t?d`
            <div class="panel-head">
              ${i?d`<div class="title-with-help"><h2>${t}</h2>${X(i.id,i.label,i.message,i.link)}</div>`:d`<h2>${t}</h2>`}
              ${a!==void 0||s.length?d`<div class="card-actions">${a!==void 0?d`<span class="card-meta">${a}</span>`:h}${La(s,p=>p.key??p.ariaLabel??p.label,Ma)}</div>`:h}
            </div>
          `:h}
      ${n}
      ${r?d`<div class="card-footer">${r}</div>`:h}
    </section>
  `}var Ra=Object.defineProperty,Na=Object.getOwnPropertyDescriptor,_e=(t,e,i,a)=>{for(var s=a>1?void 0:a?Na(e,i):e,n=t.length-1,r;n>=0;n--)(r=t[n])&&(s=(a?r(e,i,s):r(s))||s);return a&&s&&Ra(e,i,s),s};let ie=class extends I{constructor(){super(...arguments),this.channels=[],this.saving=!1,this.error=""}connectedCallback(){super.connectedCallback(),this.loadChannels()}updated(t){t.has("setup")&&this.loadChannels()}async loadChannels(){if(!(!this.setup?.cluster_ready||this.setup.phase!=="target"))try{this.channels=await b("/api/v1/channels")}catch(t){this.fail(t)}}submittedNodeName(){return this.shadowRoot?.querySelector("#setup-node-name")?.value.trim()??""}async createCluster(t){if(t.preventDefault(),!window.confirm("Create a new single-node cluster?"))return;const e=new FormData(t.currentTarget),i=String(e.get("admin_username")??"").trim(),a=String(e.get("admin_password")??"");await this.choose("/api/v1/setup/new-cluster",{node_name:this.submittedNodeName(),admin_username:i,admin_password:a},{username:i,password:a})}async joinCluster(t){t.preventDefault();const e=t.currentTarget,i=new FormData(e);await this.choose("/api/v1/cluster/join",{node_name:this.submittedNodeName(),join_link:String(i.get("join_link")??"").trim()})}async choose(t,e,i){this.saving=!0,this.error="";try{await b(t,{method:"POST",body:JSON.stringify(e)}),await this.waitForCluster(i)}catch(a){this.fail(a),this.saving=!1}}async waitForCluster(t){for(let e=0;e<120;e+=1){const{promise:i,resolve:a}=Promise.withResolvers();window.setTimeout(a,250),await i;try{t&&await b("/api/v1/auth/login",{method:"POST",body:JSON.stringify(t)});const s=await b("/api/v1/setup");if(s.cluster_ready){this.changed(s);return}}catch(s){if(!t&&s instanceof G&&s.status===401){window.location.assign("/");return}}}throw new Error("Cluster setup did not finish within 30 seconds")}async createTarget(t){t.preventDefault();const e=new FormData(t.currentTarget),i=Ie(e,e.getAll("channel_id").map(String));await this.createResource("/api/v1/targets",i)}async createResource(t,e){this.saving=!0;try{await b(t,{method:"POST",body:JSON.stringify(e)}),await this.next()}catch(i){this.fail(i),this.saving=!1}}async next(){this.saving=!0;try{this.changed(await b("/api/v1/setup/next",{method:"POST"}))}catch(t){this.fail(t),this.saving=!1}}changed(t){this.saving=!1,this.dispatchEvent(new CustomEvent("setup-changed",{detail:t,bubbles:!0,composed:!0}))}fail(t){this.error=t instanceof Error?t.message:String(t)}render(){return d`<section class="flow" aria-label="UpGrid setup" @input=${()=>this.error=""}>
      ${this.error?d`<div class="notice" role="alert">${this.error}</div>`:h}
      ${this.setup.phase==="cluster"?this.renderCluster():this.setup.phase==="channel"?this.renderChannel():this.renderTarget()}
    </section>`}renderCluster(){return d`
      <span class="eyebrow">First-run setup</span><h1>Choose your cluster</h1>
      <p class="lead">Review this node’s name, then create a new cluster or use an invitation to join one.</p>
      ${A({content:d`
          <div class="cluster-identity">
            <label for="setup-node-name">Node name<input id="setup-node-name" .value=${this.setup.node_name} required /></label>
          </div>
          <form class="cluster-create" @submit=${this.createCluster}>
            <div class="cluster-copy"><h2>Start a new cluster</h2><p>Create its first replicated administrator identity.</p></div>
            <div class="cluster-create-fields">
              <label>Administrator username<input name="admin_username" autocomplete="username" value="admin" required /></label>
              <label>Administrator password<input name="admin_password" type="password" minlength="12" autocomplete="new-password" required /></label>
            </div>
            ${E({label:this.saving?"Setting up...":"Create new cluster",busy:this.saving,error:this.error})}
          </form>
          <div class="cluster-divider"><span>Or</span></div>
          <form class="cluster-join" @submit=${this.joinCluster}>
            <div class="cluster-copy"><h2>Join an existing cluster</h2><p>Paste an <code>up://</code> join token from a current member.</p></div>
            <div class="cluster-join-fields">
              <label>Join token<input name="join_link" type="url" pattern="up://.*" placeholder="up://node.example/token" autocomplete="off" required /></label>
              ${E({label:"Join cluster",className:"secondary",busy:this.saving,error:this.error})}
            </div>
          </form>
        `})}`}renderChannel(){return d`
      <span class="eyebrow">Optional · step 2 of 3</span><h1>Add a notification channel</h1>
      <p class="lead">Send availability transitions through Telegram, SMTP, or a webhook. <span class="count">${this.setup.channel_count} already configured</span></p>
      ${A({content:d`<upgrid-notification-channel-form default-channel submit-label="Create and continue" cancel-label="Skip" .disabled=${this.saving} @channel-cancel=${this.next} @channel-saved=${this.next}></upgrid-notification-channel-form>`})}`}renderTarget(){return d`
      <span class="eyebrow">Optional · step 3 of 3</span><h1>Monitor your first target</h1>
      <p class="lead">Configure an HTTP endpoint now or continue to the dashboard. <span class="count">${this.setup.target_count} already configured</span></p>
      ${A({content:d`
          <form class="choice" @submit=${this.createTarget}>
            <label>Name<input name="name" placeholder="Production API" required /></label>
            <label>URL<input name="url" type="url" placeholder="https://example.com/health" required /></label>
            <div class="row"><label>Method<input name="method" value="GET" required /></label><label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label></div>
            <div class="row"><label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label><label>Failures before down<input name="failures" type="number" min="1" value="3" required /></label></div>
            ${this.channels.length?d`<fieldset><legend>Notification channels</legend>${this.channels.map(t=>d`<upgrid-toggle-switch name="channel_id" value=${t.id}>${t.name}</upgrid-toggle-switch>`)}</fieldset>`:d`<upgrid-empty-state>No notification channels are available</upgrid-empty-state>`}
            <div class="actions"><button class="secondary" type="button" @click=${this.next} ?disabled=${this.saving}>Skip</button>${E({label:"Create and finish",busy:this.saving,error:this.error})}</div>
          </form>
        `})}`}};ie.styles=R`
    :host { display: block; }
    *, *::before, *::after { box-sizing: border-box; }
    .flow { width: min(760px, 100%); margin: 0 auto; }
    ${Ti}
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
    .cluster-create upgrid-form-submit { justify-self: end; }
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
    @media (max-width: 620px) { .row, .cluster-create-fields, .cluster-join-fields { grid-template-columns: 1fr; } .cluster-create upgrid-form-submit, .cluster-join upgrid-form-submit { justify-self: end; } }
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
  `;_e([w({attribute:!1})],ie.prototype,"setup",2);_e([g()],ie.prototype,"channels",2);_e([g()],ie.prototype,"saving",2);_e([g()],ie.prototype,"error",2);ie=_e([z("upgrid-setup")],ie);const Ua={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 5v14m7-7l-7 7l-7-7"/>'},qa={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m5 12l7-7l7 7m-7 7V5"/>'};var Fa=Object.defineProperty,za=Object.getOwnPropertyDescriptor,Se=(t,e,i,a)=>{for(var s=a>1?void 0:a?za(e,i):e,n=t.length-1,r;n>=0;n--)(r=t[n])&&(s=(a?r(e,i,s):r(s))||s);return a&&s&&Fa(e,i,s),s};let se=class extends I{constructor(){super(...arguments),this.icon="",this.label="",this.variant="secondary",this.disabled=!1}render(){return d`
      <button class=${this.variant} type="button" aria-label=${this.label||h} title=${this.title||this.label} ?disabled=${this.disabled}>
        <iconify-icon .icon=${this.icon} aria-hidden="true"></iconify-icon>
      </button>
    `}};se.styles=R`
    :host {
      display: inline-flex;
      flex: none;
      line-height: 0;
    }
    button {
      display: grid;
      box-sizing: border-box;
      width: 44px;
      min-width: 44px;
      height: 44px;
      min-height: 44px;
      place-items: center;
      border: 1px solid var(--button-border);
      border-radius: var(--icon-button-radius, 9px);
      background: var(--button-bg);
      color: var(--button-text);
      padding: 0;
      cursor: pointer;
      font: inherit;
      user-select: none;
      transition:
        background-color 160ms ease,
        border-color 160ms ease,
        color 160ms ease,
        opacity 160ms ease,
        transform 120ms ease;
    }
    button:hover:not(:disabled) {
      border-color: var(--button-hover-border);
    }
    button:active:not(:disabled) {
      transform: translateY(1px);
    }
    button:focus-visible {
      outline: 2px solid var(--green);
      outline-offset: 2px;
    }
    button.secondary {
      border-color: var(--line);
      background: transparent;
      color: var(--muted);
    }
    button.danger {
      border-color: var(--danger-border);
      background: transparent;
      color: var(--danger-text);
    }
    button.danger:hover:not(:disabled) {
      border-color: var(--danger-text);
    }
    button.warning {
      border-color: var(--warning-border);
      background: transparent;
      color: var(--warning-text);
    }
    button.warning:hover:not(:disabled) {
      border-color: var(--warning-text);
    }
    button.success {
      border-color: var(--green);
      background: transparent;
      color: var(--green);
    }
    button.move {
      border-color: var(--line);
      background: var(--panel-2);
      color: var(--green);
    }
    button.move:hover:not(:disabled) {
      border-color: var(--green);
    }
    button:disabled {
      border-color: var(--disabled-border);
      background: var(--disabled-bg);
      color: var(--disabled-text);
      cursor: not-allowed;
      opacity: 1;
    }
    iconify-icon {
      display: inline-block;
      width: 18px;
      height: 18px;
      font-size: 18px;
    }
    @media (prefers-reduced-motion: reduce) {
      button {
        transition-duration: 0s;
      }
    }
  `;Se([w({attribute:!1})],se.prototype,"icon",2);Se([w()],se.prototype,"label",2);Se([w({reflect:!0})],se.prototype,"variant",2);Se([w({type:Boolean,reflect:!0})],se.prototype,"disabled",2);se=Se([z("upgrid-icon-button")],se);var Ha=Object.defineProperty,Ba=Object.getOwnPropertyDescriptor,He=(t,e,i,a)=>{for(var s=a>1?void 0:a?Ba(e,i):e,n=t.length-1,r;n>=0;n--)(r=t[n])&&(s=(a?r(e,i,s):r(s))||s);return a&&s&&Ha(e,i,s),s};const Va={body_contains:"Body contains",body_regex:"Body regex",json_path:"JSONPath",response_header:"Response header",latency:"Latency threshold",script:"Script"};let ae=class extends I{constructor(){super(...arguments),this.assertions=[],this.targetId="new",this.draft=[],this.loadedTarget="",this.internals=this.attachInternals()}get value(){return structuredClone(this.draft)}get validity(){return this.internals.validity}get validationMessage(){return this.internals.validationMessage}checkValidity(){return this.internals.checkValidity()}reportValidity(){return this.internals.reportValidity()}willUpdate(t){t.has("targetId")&&this.loadedTarget!==this.targetId&&(this.loadedTarget=this.targetId,this.draft=structuredClone(this.assertions))}updated(){this.internals.setFormValue(JSON.stringify(this.draft)),this.updateValidity()}formResetCallback(){this.draft=structuredClone(this.assertions),this.internals.setFormValue(JSON.stringify(this.draft))}add(){this.draft=[...this.draft,Yt("body_contains")],this.changed()}removeAssertion(t){this.draft=this.draft.filter((e,i)=>i!==t),this.changed()}move(t,e){const i=t+e;if(i<0||i>=this.draft.length)return;const a=[...this.draft];[a[t],a[i]]=[a[i],a[t]],this.draft=a,this.changed()}setKind(t,e){const i=e.currentTarget.value;this.replace(t,Yt(i))}set(t,e,i){const a=i.currentTarget,s={...this.draft[t],[e]:e==="max_ms"?Number(a.value):a.value||null};this.replace(t,s)}replace(t,e){this.draft=this.draft.map((i,a)=>a===t?e:i),this.changed()}changed(){this.internals.setFormValue(JSON.stringify(this.draft)),this.updateComplete.then(()=>{this.updateValidity(),this.dispatchEvent(new Event("input",{bubbles:!0,composed:!0}))})}updateValidity(){const t=this.renderRoot.querySelector("input:invalid, select:invalid, textarea:invalid");t?this.internals.setValidity({customError:!0},Ai(t),t):this.internals.setValidity({})}render(){return d`
      <div class="assertions">
        <button class="add" type="button" aria-label="Add assertion" @click=${this.add}>Add assertion</button>
        <div class="assertion-list">
          <slot name="required"></slot>
          ${this.draft.length?this.draft.map((t,e)=>this.renderAssertion(t,e)):d`<upgrid-empty-state>No additional assertions</upgrid-empty-state>`}
        </div>
      </div>
    `}renderAssertion(t,e){return d`
      <div class="assertion">
        <label>Type<select aria-label=${`Assertion ${e+1} type`} .value=${t.kind} @change=${i=>this.setKind(e,i)}>${Object.entries(Va).map(([i,a])=>d`<option value=${i}>${a}</option>`)}</select></label>
        ${this.renderFields(t,e)}
        <div class="actions">
          <upgrid-icon-button .icon=${qa} label=${`Move assertion ${e+1} up`} title="Move up" variant="move" ?disabled=${e===0} @click=${()=>this.move(e,-1)}></upgrid-icon-button>
          <upgrid-icon-button .icon=${Ua} label=${`Move assertion ${e+1} down`} title="Move down" variant="move" ?disabled=${e===this.draft.length-1} @click=${()=>this.move(e,1)}></upgrid-icon-button>
          <upgrid-icon-button .icon=${de} label=${`Remove assertion ${e+1}`} title="Remove assertion" variant="danger" @click=${()=>this.removeAssertion(e)}></upgrid-icon-button>
        </div>
      </div>
    `}renderFields(t,e){switch(t.kind){case"body_contains":return d`<div class="fields single"><label>Required text<input aria-label=${`Assertion ${e+1} required text`} .value=${t.value} required @input=${i=>this.set(e,"value",i)} /></label></div>`;case"body_regex":return d`<div class="fields single"><label>Regular expression<input aria-label=${`Assertion ${e+1} regular expression`} .value=${t.pattern} required @input=${i=>this.set(e,"pattern",i)} /></label></div>`;case"json_path":return d`<div class="fields"><label>Path<input aria-label=${`Assertion ${e+1} JSONPath`} .value=${t.path} placeholder="$.status" required @input=${i=>this.set(e,"path",i)} /></label><label>Expected value (optional)<input aria-label=${`Assertion ${e+1} expected value`} .value=${t.expected??""} @input=${i=>this.set(e,"expected",i)} /></label></div>`;case"response_header":return d`<div class="fields"><label>Header name<input aria-label=${`Assertion ${e+1} header name`} .value=${t.name} placeholder="content-type" required @input=${i=>this.set(e,"name",i)} /></label><label>Exact value (optional)<input aria-label=${`Assertion ${e+1} header value`} .value=${t.value??""} @input=${i=>this.set(e,"value",i)} /></label></div>`;case"latency":return d`<div class="fields single"><label>Maximum milliseconds<input aria-label=${`Assertion ${e+1} maximum milliseconds`} type="number" min="1" .value=${String(t.max_ms)} required @input=${i=>this.set(e,"max_ms",i)} /></label></div>`;case"script":return d`<div class="fields single"><label><span class="title-with-help">Boolean Rhai expression ${X(`script-assertion-${e+1}-help`,`About script assertion ${e+1}`,"Return true to pass. The script can read the response status, latency, body, final URL, and headers.",{href:"https://upgrid.rs/reference/script-assertions/",label:"Read the script assertion reference"})}</span><textarea aria-label=${`Assertion ${e+1} script`} required @input=${i=>this.set(e,"source",i)}>${t.source}</textarea></label></div>`;default:return h}}};ae.formAssociated=!0;ae.styles=R`
    ${ft}
    :host { display: grid; gap: 10px; }
    .assertions, .assertion-list { display: grid; gap: 10px; }
    .assertion-list { max-height: min(420px, 50vh); overflow-y: auto; padding-right: 4px; scrollbar-gutter: stable; }
    ::slotted(.required-assertion) { display: grid; grid-template-columns: minmax(140px, 0.7fr) minmax(180px, 1.3fr) auto; gap: 8px; align-items: end; }
    .assertion { display: grid; grid-template-columns: minmax(140px, 0.7fr) minmax(180px, 1.3fr) auto; gap: 8px; align-items: end; }
    .fields { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }
    .fields.single { grid-template-columns: 1fr; }
    label { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
    input, select, textarea { box-sizing: border-box; width: 100%; min-height: 44px; border: 1px solid var(--line); border-radius: 9px; background: var(--input-bg); color: var(--text); padding: 9px 10px; font-family: inherit; font-size: 16px; }
    textarea { min-height: 72px; resize: vertical; font-family: ui-monospace, monospace; }
    .actions { display: flex; align-items: flex-end; gap: 4px; }
    button { border: 1px solid var(--line); border-radius: 7px; background: var(--panel-2); color: var(--text); padding: 8px 10px; cursor: pointer; user-select: none; }
    button.add:disabled { border-color: var(--disabled-border); background: var(--disabled-bg); color: var(--disabled-text); cursor: not-allowed; opacity: 1; }
    .add { display: inline-flex; min-height: 34px; align-items: center; gap: 6px; justify-self: end; border-color: var(--line); background: var(--panel-2); color: var(--text); padding: 6px 10px; font-size: 13px; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, transform 120ms ease; }
    .add::before { color: var(--green); content: "+"; font-size: 18px; font-weight: 400; line-height: 12px; }
    .add:hover { border-color: var(--green); color: var(--green); }
    .add:active { transform: translateY(1px); }
    @media (max-width: 720px) { .assertion, ::slotted(.required-assertion) { grid-template-columns: 1fr; } .fields { grid-template-columns: 1fr; } }
  `;He([w({attribute:!1})],ae.prototype,"assertions",2);He([w({attribute:"target-id"})],ae.prototype,"targetId",2);He([g()],ae.prototype,"draft",2);ae=He([z("upgrid-http-assertion-editor")],ae);function Yt(t){switch(t){case"body_contains":return{kind:t,value:""};case"body_regex":return{kind:t,pattern:""};case"json_path":return{kind:t,path:"$",expected:null};case"response_header":return{kind:t,name:"",value:null};case"latency":return{kind:t,max_ms:1e3};case"script":return{kind:t,source:"status == 200"}}}const Ja={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3a6 6 0 0 0 9 9a9 9 0 1 1-9-9Z"/>'},Ga={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><rect width="20" height="14" x="2" y="3" rx="2"/><path d="M8 21h8m-4-4v4"/></g>'},Ka={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M4.93 4.93l1.41 1.41m11.32 11.32l1.41 1.41M2 12h2m16 0h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/></g>'};var Wa=Object.defineProperty,x=(t,e,i,a)=>{for(var s=void 0,n=t.length-1,r;n>=0;n--)(r=t[n])&&(s=r(e,i,s)||s);return s&&Wa(e,i,s),s};const Oe=["system","dark","bright"],Qe={system:Ga,dark:Ja,bright:Ka},Y={overview:"/",alerts:"/alerts",cluster:"/cluster",trash:"/trash",manage:"/admin/manage",changePassword:"/admin/change-password",users:"/admin/users",apiTokens:"/admin/api-tokens"};function Zt(){return Object.entries(Y).find(([,t])=>t===window.location.pathname)?.[0]??"overview"}function Qa(){const t=localStorage.getItem("upgrid-theme");return Oe.includes(t)?t:"system"}class y extends I{constructor(){super(...arguments),this.targets=[],this.trashedTargets=[],this.channels=[],this.alerts=[],this.transitions=[],this.secrets=[],this.joinTokens=[],this.identities=[],this.apiTokens=[],this.authReady=!1,this.newApiToken="",this.error="",this.targetError="",this.live=!1,this.saving=!1,this.historyLoading=!1,this.joinUrl="",this.alertSearch="",this.alertDeliveryFilter="all",this.alertKindFilter="all",this.alertAcknowledgedFilter="all",this.search="",this.statusFilter="all",this.sort="name",this.selectedIds=new Set,this.activeSection=Zt(),this.copied=!1,this.setupMode=!1,this.warningDismissed=sessionStorage.getItem("upgrid-warning-dismissed")==="1",this.unlimitedUses=!1,this.theme=Qa(),this.detailDirty=!1,this.detailTab="details",this.publicStatusGeneration=0,this.detailInitialState="",this.systemTheme=matchMedia("(prefers-color-scheme: light)"),this.systemThemeChanged=()=>{this.theme==="system"&&this.applyTheme()},this.routeChanged=()=>{if(this.setupMode&&this.setup){window.history.replaceState(null,"",this.setup.path);return}this.activeSection=Zt()},this.backgroundClicked=e=>{const i=this.renderRoot.querySelector(".account-menu");i?.open&&!e.composedPath().includes(i)&&(i.open=!1)},this.sessionExpired=()=>{this.events?.close(),this.events=void 0,this.stopPublicStatus(),this.session=void 0,this.settings=void 0,this.setupMode=!1,this.saving=!1,this.error="",this.targetError="",this.activeSection="overview",window.history.replaceState(null,"","/")}}connectedCallback(){super.connectedCallback(),this.applyTheme(),this.systemTheme.addEventListener("change",this.systemThemeChanged),window.addEventListener("popstate",this.routeChanged),document.addEventListener("pointerdown",this.backgroundClicked),window.addEventListener(st,this.sessionExpired),this.start()}disconnectedCallback(){this.systemTheme.removeEventListener("change",this.systemThemeChanged),window.removeEventListener("popstate",this.routeChanged),document.removeEventListener("pointerdown",this.backgroundClicked),window.removeEventListener(st,this.sessionExpired),this.events?.close(),this.stopPublicStatus(),super.disconnectedCallback()}async start(){try{const e=await b("/api/v1/setup");e.cluster_ready&&(this.session=await b("/api/v1/auth/session")),await this.activate(e)}catch(e){if(e instanceof G&&e.status===401&&window.location.pathname==="/")try{await this.activatePublicStatus()}catch(i){(!(i instanceof G)||i.status!==401)&&(this.error=i instanceof Error?i.message:String(i))}else this.error=e instanceof Error?e.message:String(e)}this.authReady=!0}async activatePublicStatus(){const e=++this.publicStatusGeneration,i=await b("/api/v1/status");e===this.publicStatusGeneration&&(this.publicStatus=i,this.live=!0,this.publicStatusTimer!==void 0&&window.clearInterval(this.publicStatusTimer),this.publicStatusTimer=window.setInterval(()=>{this.refreshPublicStatus()},3e4))}async refreshPublicStatus(){const e=++this.publicStatusGeneration;try{const i=await b("/api/v1/status");if(e!==this.publicStatusGeneration)return;this.publicStatus=i,this.live=!0}catch(i){if(e!==this.publicStatusGeneration)return;this.live=!1,i instanceof G&&i.status===401&&this.stopPublicStatus()}}stopPublicStatus(){this.publicStatusGeneration+=1,this.publicStatusTimer!==void 0&&window.clearInterval(this.publicStatusTimer),this.publicStatusTimer=void 0,this.publicStatus=void 0,this.live=!1}showLogin(){this.stopPublicStatus()}async activate(e){if(this.setup=e,this.setupMode=e.setup,this.setupMode){window.history.replaceState(null,"",e.path),e.cluster_ready?(await this.refresh(),this.connectEvents()):this.live=!0;return}await this.refresh(),this.connectEvents()}async login(e){e.preventDefault();const i=new FormData(e.currentTarget);this.saving=!0,this.error="";try{this.session=await b("/api/v1/auth/login",{method:"POST",body:JSON.stringify({username:String(i.get("username")??""),password:String(i.get("password")??"")})}),this.stopPublicStatus(),await this.activate(await b("/api/v1/setup"))}catch(a){this.error=a instanceof Error?a.message:String(a)}finally{this.saving=!1}}async logout(){await b("/api/v1/auth/logout",{method:"POST"}),this.events?.close(),this.stopPublicStatus(),this.session=void 0,this.settings=void 0,this.setupMode=!1,window.history.replaceState(null,"","/");try{await this.activatePublicStatus()}catch(e){(!(e instanceof G)||e.status!==401)&&(this.error=e instanceof Error?e.message:String(e))}}connectEvents(){this.events?.close(),this.events=new EventSource("/api/v1/events"),this.events.addEventListener("state",()=>{this.refresh()}),this.events.onopen=()=>this.live=!0,this.events.onerror=()=>this.live=!1}applyTheme(){const e=this.theme==="system"?this.systemTheme.matches?"bright":"dark":this.theme;this.dataset.theme=e,document.querySelector('meta[name="theme-color"]')?.setAttribute("content",e==="bright"?"#f4f8f6":"#0b1110")}cycleTheme(){this.theme=Oe[(Oe.indexOf(this.theme)+1)%Oe.length],localStorage.setItem("upgrid-theme",this.theme),this.applyTheme()}dismissWarning(){sessionStorage.setItem("upgrid-warning-dismissed","1"),this.warningDismissed=!0}async refresh(){try{[this.targets,this.trashedTargets,this.channels,this.alerts,this.transitions,this.secrets,this.cluster,this.joinTokens,this.identities,this.apiTokens,this.settings]=await Promise.all([b("/api/v1/targets"),b("/api/v1/trash/targets"),b("/api/v1/channels"),b("/api/v1/alerts"),b("/api/v1/transitions"),b("/api/v1/secrets"),b("/api/v1/cluster"),b("/api/v1/join-tokens"),b("/api/v1/identities"),b("/api/v1/api-tokens"),b("/api/v1/settings")]),this.error=""}catch(e){this.error=e instanceof Error?e.message:String(e)}}openTargetDialog(){this.targetError="",this.renderRoot.querySelector("#target-dialog")?.showModal()}closeTargetDialog(){this.targetError="",this.renderRoot.querySelector("#target-dialog")?.close()}openTarget(e){this.targetError="",this.detailDirty=!1,this.detailTab="details",this.selected=e,this.targetHistory=void 0,this.historyLoading=!0,this.loadTargetHistory(e.id),this.updateComplete.then(()=>{const i=this.renderRoot.querySelector("#detail-dialog"),a=i?.querySelector("form");a&&(this.detailInitialState=this.detailFormState(a)),i?.showModal()})}async loadTargetHistory(e){try{const i=await b(`/api/v1/targets/${e}/history?limit=720`);this.selected?.id===e&&(this.targetHistory=i)}catch(i){this.selected?.id===e&&(this.error=i instanceof Error?i.message:String(i))}finally{this.selected?.id===e&&(this.historyLoading=!1)}}closeDetailDialog(){this.targetError="",this.renderRoot.querySelector("#detail-dialog")?.close(),this.detailDirty=!1,this.detailTab="details",this.detailInitialState="",this.selected=void 0,this.targetHistory=void 0,this.historyLoading=!1}showDialog(e){this.renderRoot.querySelector(`#${e}`)?.showModal()}dismissOnBackdrop(e){const i=e.currentTarget;e.target===i&&(i.close(),i.id==="detail-dialog"&&this.closeDetailDialog())}navigate(e,i){e.preventDefault(),this.activeSection=i,window.history.pushState(null,"",Y[i]),this.renderRoot.querySelector(".account-menu")?.removeAttribute("open")}closeDialog(e){this.renderRoot.querySelector(`#${e}`)?.close()}selectDetailTab(e){this.detailTab=e}detailFormState(e){return JSON.stringify([...new FormData(e).entries()])}compareDetailForm(e){this.detailDirty=this.detailFormState(e)!==this.detailInitialState}updateDetailDirty(e){this.targetError="",this.compareDetailForm(e.currentTarget)}}x([g()],y.prototype,"targets");x([g()],y.prototype,"trashedTargets");x([g()],y.prototype,"channels");x([g()],y.prototype,"alerts");x([g()],y.prototype,"transitions");x([g()],y.prototype,"secrets");x([g()],y.prototype,"cluster");x([g()],y.prototype,"joinTokens");x([g()],y.prototype,"identities");x([g()],y.prototype,"apiTokens");x([g()],y.prototype,"settings");x([g()],y.prototype,"publicStatus");x([g()],y.prototype,"session");x([g()],y.prototype,"authReady");x([g()],y.prototype,"newApiToken");x([g()],y.prototype,"editingIdentity");x([g()],y.prototype,"error");x([g()],y.prototype,"targetError");x([g()],y.prototype,"live");x([g()],y.prototype,"saving");x([g()],y.prototype,"selected");x([g()],y.prototype,"targetHistory");x([g()],y.prototype,"historyLoading");x([g()],y.prototype,"editingChannel");x([g()],y.prototype,"joinUrl");x([g()],y.prototype,"alertSearch");x([g()],y.prototype,"alertDeliveryFilter");x([g()],y.prototype,"alertKindFilter");x([g()],y.prototype,"alertAcknowledgedFilter");x([g()],y.prototype,"search");x([g()],y.prototype,"statusFilter");x([g()],y.prototype,"sort");x([g()],y.prototype,"selectedIds");x([g()],y.prototype,"activeSection");x([g()],y.prototype,"copied");x([g()],y.prototype,"setupMode");x([g()],y.prototype,"setup");x([g()],y.prototype,"warningDismissed");x([g()],y.prototype,"unlimitedUses");x([g()],y.prototype,"theme");x([g()],y.prototype,"detailDirty");x([g()],y.prototype,"detailTab");class Ya extends y{async createTarget(e){e.preventDefault();const i=e.currentTarget,a=new FormData(i),s=i.querySelector("upgrid-http-assertion-editor")?.value??[],n=Ie(a,a.getAll("channel_id").map(String),a.get("use_default_channels")==="on",void 0,s);this.targetError="",this.saving=!0;try{await b("/api/v1/targets",{method:"POST",body:JSON.stringify(n)}),i.reset(),this.closeTargetDialog(),await this.refresh()}catch(r){this.targetError=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}async updateTarget(e){if(e.preventDefault(),!this.selected)return;const i=new FormData(e.currentTarget),a=e.currentTarget.querySelector("upgrid-http-assertion-editor")?.value??[];let s=`/api/v1/nodes/${this.selected.id}`,n={name:String(i.get("name"))};this.selected.kind==="http"&&(s=`/api/v1/targets/${this.selected.id}`,n={...Ie(i,i.getAll("channel_id").map(String),i.get("use_default_channels")==="on","http",a),headers:Object.fromEntries(Object.entries(this.selected.headers).map(([r,o])=>[r,o.kind==="literal"?o.value:{secret_id:o.secret_id}])),body:this.selected.body?.kind==="literal"?this.selected.body.value:this.selected.body?{secret_id:this.selected.body.secret_id}:null}),this.selected.kind!=="http"&&this.selected.kind!=="node"&&(s=`/api/v1/targets/${this.selected.id}`,n=Ie(i,i.getAll("channel_id").map(String),i.get("use_default_channels")==="on",this.selected.kind,a)),this.targetError="",this.saving=!0;try{await b(s,{method:"PUT",body:JSON.stringify(n)}),this.closeDetailDialog(),await this.refresh()}catch(r){this.targetError=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}async deleteTarget(){if(!(!this.selected||!window.confirm("Move this target and its history to trash? You can restore it before its retention period expires."))){this.saving=!0;try{await b(`/api/v1/targets/${this.selected.id}`,{method:"DELETE"}),this.closeDetailDialog(),await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async restoreTarget(e){window.confirm(`Restore ${e.name} with its settings and history?`)&&await this.saveResource(()=>b(`/api/v1/trash/targets/${e.id}/restore`,{method:"POST"}))}async purgeTarget(e){window.confirm(`Permanently delete ${e.name} and all of its history? This cannot be undone.`)&&await this.saveResource(()=>b(`/api/v1/trash/targets/${e.id}`,{method:"DELETE"}))}async setPaused(e){if(this.selected){this.saving=!0;try{await b(`/api/v1/targets/${this.selected.id}/${e?"pause":"resume"}`,{method:"POST"}),this.closeDetailDialog(),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}}async createSecret(e){e.preventDefault();const i=e.currentTarget,a=new FormData(i);this.saving=!0;try{await b("/api/v1/secrets",{method:"POST",body:JSON.stringify({name:a.get("name"),value:a.get("value")})}),i.reset(),this.closeDialog("secret-dialog"),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}openChannelDialog(e){this.editingChannel=e,this.showDialog("channel-dialog")}async channelSaved(e){const i=e.detail;this.channels=this.channels.some(({id:a})=>a===i.id)?this.channels.map(a=>a.id===i.id?i:a):[...this.channels,i],this.editingChannel=void 0,this.closeDialog("channel-dialog"),await this.refresh()}async setChannelDefault(e,i){try{await b(`/api/v1/channels/${e.id}/default`,{method:"PUT",body:JSON.stringify({default:i})}),await this.refresh()}catch(a){this.error=a instanceof Error?a.message:String(a)}}openTokenDialog(){this.unlimitedUses=!1,this.showDialog("token-config-dialog")}async createJoinToken(e){e.preventDefault();const i=new FormData(e.currentTarget);this.saving=!0;try{const a=await b("/api/v1/join-tokens",{method:"POST",body:JSON.stringify({expires_in_seconds:Number(i.get("expiration_days"))*86400,max_uses:this.unlimitedUses?null:Number(i.get("max_uses"))})});this.joinUrl=a.url,this.copied=!1,await this.refresh(),this.closeDialog("token-config-dialog"),this.showDialog("join-dialog")}catch(a){this.error=a instanceof Error?a.message:String(a)}finally{this.saving=!1}}passwordsMatch(e){const i=e.elements.namedItem("password"),a=e.elements.namedItem("password_confirmation");return!i||!a?!0:(a.setCustomValidity(i.value===a.value?"":"Passwords do not match."),e.reportValidity())}async createIdentity(e){e.preventDefault();const i=e.currentTarget;if(!this.passwordsMatch(i))return;const a=new FormData(i);await this.saveResource(async()=>{await b("/api/v1/identities",{method:"POST",body:JSON.stringify({username:String(a.get("username")??""),password:String(a.get("password")??"")})}),i.reset(),this.closeDialog("add-user-dialog")})}async updateIdentity(e,i){i.preventDefault();const a=i.currentTarget;if(!this.passwordsMatch(a))return;const s=new FormData(a),n=String(s.get("password")??"");await this.saveResource(async()=>{await b(`/api/v1/identities/${e.id}`,{method:"PUT",body:JSON.stringify({username:String(s.get("username")??""),password:n||null})}),e.id===this.session?.identity_id&&n?await this.logout():(this.closeDialog("edit-user-dialog"),this.editingIdentity=void 0)})}async deleteIdentity(e){window.confirm(`Delete identity ${e.username}? Its API Tokens will also be revoked.`)&&await this.saveResource(()=>b(`/api/v1/identities/${e.id}`,{method:"DELETE"}))}async createApiToken(e){e.preventDefault();const i=e.currentTarget,a=new FormData(i);await this.saveResource(async()=>{const s=Number(a.get("expires_in_days")),n=await b("/api/v1/api-tokens",{method:"POST",body:JSON.stringify({name:String(a.get("name")??""),expires_in_seconds:s?s*86400:null})});this.newApiToken=n.value,i.reset(),this.closeDialog("api-token-dialog")})}async revokeApiToken(e){window.confirm(`Revoke API token ${e.name}?`)&&await this.saveResource(()=>b(`/api/v1/api-tokens/${e.id}`,{method:"DELETE"}))}async setNodeDrain(e,i){await this.saveResource(()=>b(`/api/v1/nodes/${e.id}/drain`,{method:"PUT",body:JSON.stringify({draining:i,force:!1})}))}async removeNode(e,i){const a=i?`Replace failed node ${e.name}? Confirm that it is permanently stopped. Its assignments will be released immediately.`:`Remove drained node ${e.name} from the cluster?`;window.confirm(a)&&(await this.saveResource(()=>b(`/api/v1/nodes/${e.id}?force=${i}`,{method:"DELETE"})),i&&!this.error&&this.openTokenDialog())}async acknowledgeAlert(e){await this.updateAlert("acknowledge",e)}async retryAlert(e){await this.updateAlert("retry",e)}async updateAlert(e,i){await this.saveResource(()=>b(`/api/v1/alerts/${e}`,{method:"POST",body:JSON.stringify({target_id:i.target_id,channel_id:i.channel_id,scheduled_at_ms:i.scheduled_at_ms,kind:i.kind})}))}async updateSettings(e){e.preventDefault();const i=new FormData(e.currentTarget);await this.saveResource(()=>b("/api/v1/settings",{method:"PUT",body:JSON.stringify({public_status_enabled:i.get("public_status_enabled")==="on"})}))}async saveResource(e){this.saving=!0,this.error="";try{await e(),this.session&&await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}async setupChanged(e){const i=e.detail;if(this.setup=i,this.setupMode=i.setup,window.history.replaceState(null,"",i.path),i.setup){i.cluster_ready&&(this.session=await b("/api/v1/auth/session"),await this.refresh(),this.connectEvents());return}this.activeSection="overview",await this.refresh(),this.connectEvents()}async revokeJoinToken(e){if(window.confirm("Revoke this join token? Nodes using it will no longer be admitted.")){this.saving=!0;try{await b(`/api/v1/join-tokens/${e.id}`,{method:"DELETE"}),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}}async copyJoinUrl(){let e=!1;try{await navigator.clipboard.writeText(this.joinUrl),e=!0}catch{const i=Object.assign(document.createElement("textarea"),{value:this.joinUrl});i.style.cssText="position: fixed; opacity: 0",document.body.append(i),i.select(),e=document.execCommand("copy"),i.remove()}if(!e){this.error="Could not copy the join URL";return}this.copied=!0,window.setTimeout(()=>this.copied=!1,2e3)}toggleSelected(e,i){const a=new Set(this.selectedIds);i?a.add(e):a.delete(e),this.selectedIds=a}async bulkPause(e){this.saving=!0;try{await Promise.all([...this.selectedIds].map(i=>b(`/api/v1/targets/${i}/${e?"pause":"resume"}`,{method:"POST"}))),this.selectedIds=new Set,await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}async bulkDelete(){if(window.confirm(`Move ${this.selectedIds.size} selected Targets and their history to Trash?`)){this.saving=!0;try{await Promise.all([...this.selectedIds].map(e=>b(`/api/v1/targets/${e}`,{method:"DELETE"}))),this.selectedIds=new Set,await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async cleanupSecrets(){const e=this.secrets.filter(i=>!i.referenced);!e.length||!window.confirm(`Permanently delete ${e.length} unused ${e.length===1?"Secret":"Secrets"}? References are checked again when cleanup commits.`)||await this.saveResource(()=>b("/api/v1/secrets/unreferenced",{method:"DELETE"}))}async deleteResource(e,i,a){if(window.confirm(`Delete ${a}?`))try{await b(`/api/v1/${e}/${i}`,{method:"DELETE"}),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}}}function Ci(t,e){return d`
    <div class="brand">
      <a class="brand-link" href="/" aria-label="UpGrid overview" @click=${e??h}><img src="/favicon.svg" alt="UpGrid" /></a>
      <span class="live"><i class=${`status-dot${t?" online":""}`}></i>${t?"Online":"Offline"}</span>
    </div>
  `}const Za={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 3a2.85 2.83 0 1 1 4 4L7.5 20.5L2 22l1.5-5.5Zm-2 2l4 4"/>'};function Xa(t,e){const i=e.search.trim().toLocaleLowerCase();return(!i||`${t.target_name} ${t.channel_name}`.toLocaleLowerCase().includes(i))&&(e.delivery==="all"||t.delivery===e.delivery)&&(e.kind==="all"||t.kind===e.kind)&&(e.acknowledged==="all"||(e.acknowledged==="yes"?t.acknowledged_at_ms!==null:t.acknowledged_at_ms===null))}function er(t){return t.delivery==="pending"?t.next_attempt_at_ms===null?`${t.attempts} attempts`:`${t.attempts} attempts · next ${new Date(t.next_attempt_at_ms).toLocaleString()}`:t.delivery==="failed"?t.diagnostic??"Delivery failed":t.completed_at_ms===null?"Delivered":`Delivered ${new Date(t.completed_at_ms).toLocaleString()}`}function tr(t,e,i,a,s,n){const r=t.filter(o=>Xa(o,a));return d`
    <section class="heading" id="alerts">
      <div><span class="eyebrow">Delivery history</span><h1>Alerts</h1></div>
      <button class="button" @click=${n.create}>Add channel</button>
    </section>
    ${A({title:"Notification deliveries",label:"Alert history",metadata:`${r.length} of ${t.length} alerts`,className:"alert-history",content:d`
        <div class="alert-filters">
          <label>Search<input type="search" .value=${a.search} placeholder="Target or channel" @input=${o=>n.setSearch(o.target.value)} /></label>
          <label>Delivery<select .value=${a.delivery} @change=${o=>n.setDelivery(o.target.value)}><option value="all">All</option><option value="pending">Pending</option><option value="delivered">Delivered</option><option value="failed">Failed</option></select></label>
          <label>Transition<select .value=${a.kind} @change=${o=>n.setKind(o.target.value)}><option value="all">All</option><option value="down">Down</option><option value="recovered">Recovered</option></select></label>
          <label>Acknowledged<select .value=${a.acknowledged} @change=${o=>n.setAcknowledged(o.target.value)}><option value="all">All</option><option value="no">No</option><option value="yes">Yes</option></select></label>
        </div>
        ${r.length?r.map(o=>d`
                  <div class="resource alert-resource">
                    <div class="alert-summary">
                      <div class="channel-title">
                        <strong>${o.target_name}</strong>
                        <span class=${`badge ${o.kind==="recovered"?"up":"down"}`}>${o.kind}</span>
                        <span class="badge">${o.delivery}</span>
                        ${o.acknowledged_at_ms===null?h:d`<span class="badge">acknowledged</span>`}
                      </div>
                      <code>${o.channel_name} · ${new Date(o.scheduled_at_ms).toLocaleString()}</code>
                      <span class="meta">${er(o)}</span>
                    </div>
                    <div class="alert-actions">
                      ${o.delivery==="failed"?d`<button class="button secondary" ?disabled=${s} @click=${()=>n.retry(o)}>Retry</button>`:h}
                      ${o.acknowledged_at_ms===null?d`<button class="button secondary" ?disabled=${s} @click=${()=>n.acknowledge(o)}>Acknowledge</button>`:h}
                    </div>
                  </div>
                `):d`<upgrid-empty-state>No alerts match these filters</upgrid-empty-state>`}
      `})}
    <div class="page-columns">
      ${A({title:"Availability transitions",label:"Availability history",metadata:`${e.length} events`,content:d`
          ${e.length?e.map(o=>{const l=o.kind==="recovered"?"up":"down";return d`
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
                  `}):d`<upgrid-empty-state>No availability transitions</upgrid-empty-state>`}
        `})}
      ${A({title:"Notification channels",metadata:`${i.length} configured`,content:d`
          ${i.length?i.map(o=>d`
                    <div class="resource channel-resource">
                      <div class="channel-summary"><div class="channel-title"><strong>${o.name}</strong><span class="badge">${o.kind}</span></div><code>${o.destination}</code></div>
                      <div class="channel-actions">
                        <upgrid-toggle-switch aria-label=${`Default channel ${o.name}`} .checked=${o.default} @change=${l=>n.setDefault(o,l.currentTarget.checked)}>Default</upgrid-toggle-switch>
                        <upgrid-icon-button .icon=${Za} label=${`Edit channel ${o.name}`} title=${`Edit ${o.name}`} @click=${()=>n.edit(o)}></upgrid-icon-button>
                        <upgrid-icon-button .icon=${de} label=${`Delete channel ${o.name}`} title=${`Delete ${o.name}`} variant="danger" @click=${()=>n.remove(o)}></upgrid-icon-button>
                      </div>
                    </div>
                  `):d`<upgrid-empty-state>No notification channels</upgrid-empty-state>`}
        `})}
    </div>
  `}function rt(t){const e=t.currentTarget,i=e.elements.namedItem("password"),a=e.elements.namedItem("password_confirmation");!(i instanceof HTMLInputElement)||!(a instanceof HTMLInputElement)||a.setCustomValidity(a.value&&a.value!==i.value?"Passwords do not match.":"")}function ir(t,e,i,a){return d`
    <main class="shell setup-shell">
      <header>
        ${Ci(t)}
      </header>
      ${A({label:"Sign in",className:"auth-panel",content:d`
          <form class="choice" @submit=${a.login} @input=${a.changed}>
            <div><span class="eyebrow">Cluster access</span><h1 id="login-title">Sign in</h1><p class="meta">Use a replicated operator identity.</p></div>
            ${i?d`<div class="notice" role="alert">${i}</div>`:h}
            <label>Username<input name="username" autocomplete="username" required autofocus /></label>
            <label>Password<input name="password" type="password" autocomplete="current-password" required /></label>
            <div class="dialog-actions">${E({label:e?"Signing in...":"Sign in",busy:e,error:i})}</div>
          </form>
        `})}
    </main>`}function sr(t,e,i,a){return t?d`
    <div class="admin-page change-password-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>Change password</h1></div></div>
      ${A({className:"auth-panel",content:d`
          <form class="choice" @submit=${s=>a.updateIdentity(t,s)} @input=${s=>{rt(s),a.changed()}}>
            <input name="username" type="hidden" .value=${t.username} />
            <label>Username<input .value=${t.username} autocomplete="username" disabled /></label>
            <label>New password<input name="password" type="password" minlength="12" autocomplete="new-password" required autofocus /></label>
            <label>Confirm new password<input name="password_confirmation" type="password" minlength="12" autocomplete="new-password" required /></label>
            <div class="dialog-actions">${E({label:"Change password",busy:e,error:i})}</div>
          </form>
        `})}
    </div>`:d`<upgrid-empty-state>Current identity unavailable</upgrid-empty-state>`}function ar(t,e,i,a,s,n){return d`
    <div class="admin-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>Users</h1></div><button class="button" type="button" @click=${n.openAddUser}>Add user</button></div>
      ${A({title:"Operator identities",metadata:`${t.length} administrators`,content:d`
          ${t.map(r=>d`
              <div class="resource user-resource">
                <button class="resource-main" type="button" aria-label=${`Edit user ${r.username}`} ?disabled=${a} @click=${()=>n.openEditUser(r)}>
                  <span>
                    <strong>${r.username}</strong>
                    <code>Operator identity${r.id===e?" · current user":""}</code>
                  </span>
                </button>
                <upgrid-icon-button .icon=${de} label=${`Delete user ${r.username}`} title=${`Delete ${r.username}`} variant="danger" ?disabled=${r.id===e||a} @click=${()=>n.deleteIdentity(r)}></upgrid-icon-button>
              </div>
            `)}
        `})}
    </div>
    <dialog id="add-user-dialog" aria-labelledby="add-user-title" @click=${n.dismissDialog}>
      <div class="dialog-head"><h2 id="add-user-title">Add user</h2></div>
      <form @submit=${n.createIdentity} @input=${r=>{rt(r),n.changed()}}>
        <label>Username<input name="username" autocomplete="username" required autofocus /></label>
        <label>Password<input name="password" type="password" minlength="12" autocomplete="new-password" required /></label>
        <label>Confirm password<input name="password_confirmation" type="password" minlength="12" autocomplete="new-password" required /></label>
        <div class="dialog-actions"><button class="button secondary" type="button" @click=${n.closeAddUser}>Cancel</button>${E({label:a?"Adding...":"Add user",busy:a,error:s})}</div>
      </form>
    </dialog>
    ${i?d`
          <dialog id="edit-user-dialog" aria-labelledby="edit-user-title" @click=${n.dismissDialog}>
            <div class="dialog-head"><h2 id="edit-user-title">Edit user</h2></div>
            <form @submit=${r=>n.updateIdentity(i,r)} @input=${r=>{rt(r),n.changed()}}>
              <label>Username<input name="username" .value=${i.username} autocomplete="username" required autofocus /></label>
              <label>New password<input name="password" type="password" minlength="12" autocomplete="new-password" placeholder="Keep current password" /></label>
              <label>Confirm new password<input name="password_confirmation" type="password" minlength="12" autocomplete="new-password" placeholder="Keep current password" /></label>
              <div class="dialog-actions"><button class="button secondary" type="button" @click=${n.closeEditUser}>Cancel</button>${E({label:"Save changes",busy:a,error:s,trackChanges:!0})}</div>
            </form>
          </dialog>`:h}`}function rr(t,e,i,a,s){return d`
    <div class="admin-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>API tokens</h1></div><button class="button" type="button" @click=${s.openApiToken}>New token</button></div>
      ${A({title:"API tokens",metadata:`${t.length} active`,content:d`
          ${e?d`<div class="notice token-value" role="status"><strong>Copy this token now.</strong><code>${e}</code><button class="button secondary" @click=${s.dismissToken}>Dismiss</button></div>`:h}
          ${t.length?t.map(n=>d`<div class="resource"><div><strong>${n.name}</strong><code>${n.expires_at_ms?`Expires ${new Date(n.expires_at_ms).toLocaleString()}`:"Never expires"}</code></div><button class="button danger" @click=${()=>s.revokeApiToken(n)}>Revoke</button></div>`):d`<upgrid-empty-state>No API tokens</upgrid-empty-state>`}
        `})}
    </div>
    <dialog id="api-token-dialog" aria-labelledby="api-token-title" @click=${s.dismissDialog}>
      <div class="dialog-head"><h2 id="api-token-title">New API token</h2></div>
      <form @submit=${s.createApiToken} @input=${s.changed}>
        <label>Name<input name="name" placeholder="Automation" required autofocus /></label>
        <label>Expires in days<input name="expires_in_days" type="number" min="1" max="365" placeholder="Never" /></label>
        <div class="dialog-actions"><button class="button secondary" type="button" @click=${s.closeApiToken}>Cancel</button>${E({label:i?"Creating...":"Create API token",busy:i,error:a})}</div>
      </form>
    </dialog>`}function nr(t,e,i,a,s){return d`
    <div class="admin-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>Manage</h1></div></div>
      ${A({title:"Public status access",content:d`
          <form @submit=${a} @input=${s}>
            <upgrid-toggle-switch name="public_status_enabled" .checked=${t?.public_status_enabled??!1} ?disabled=${t===void 0||e}>
              <span class="setting-copy">
                Allow status viewing without login
                <small>External visitors can see target names, states, and recent evaluation metrics. URLs, configuration, alerts, cluster data, and administration remain private.</small>
              </span>
            </upgrid-toggle-switch>
            <div class="dialog-actions">${E({label:e?"Saving...":"Save changes",busy:e,blocked:t===void 0,error:i,baselineKey:String(t?.public_status_enabled),blockedMessage:"Settings are unavailable",trackChanges:!0})}</div>
          </form>
        `})}
    </div>`}const or={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><path d="M15 22v-4a4.8 4.8 0 0 0-1-3.5c3 0 6-2 6-5.5c.08-1.25-.27-2.48-1-3.5c.28-1.15.28-2.35 0-3.5c0 0-1 0-3 1.5c-2.64-.5-5.36-.5-8 0C6 2 5 2 5 2c-.3 1.15-.3 2.35 0 3.5A5.403 5.403 0 0 0 4 9c0 3.5 3 5.5 6 5.5c-.39.49-.68 1.05-.85 1.65c-.17.6-.22 1.23-.15 1.85v4"/><path d="M9 18c-4.51 2-5-2-7-2"/></g>'},lr={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><path d="M21.54 15H17a2 2 0 0 0-2 2v4.54M7 3.34V5a3 3 0 0 0 3 3v0a2 2 0 0 1 2 2v0c0 1.1.9 2 2 2v0a2 2 0 0 0 2-2v0c0-1.1.9-2 2-2h3.17M11 21.95V18a2 2 0 0 0-2-2v0a2 2 0 0 1-2-2v-1a2 2 0 0 0-2-2H2.05"/><circle cx="12" cy="12" r="10"/></g>'};function Te(){return d`
    <footer aria-label="Project information">
      <div class="footer-links">
        <a href="https://miao.dev">A project by Pop</a>
        <span aria-hidden="true">|</span>
        <a href="https://github.com/George-Miao/UpGrid">
          <iconify-icon .icon=${or} aria-hidden="true"></iconify-icon>GitHub
        </a>
        <span aria-hidden="true">|</span>
        <a href="https://upgrid.rs">
          <iconify-icon .icon=${lr} aria-hidden="true"></iconify-icon>upgrid.rs
        </a>
      </div>
      <div class="footer-powered">
        Proudly powered by <a href="https://compio.rs/">Compio</a> and
        <a href="https://github.com/databendlabs/openraft">OpenRaft</a>
      </div>
    </footer>
  `}function Ei(t,e=[],i=!0){return d`
    <div class="channel-fields">
      <upgrid-toggle-switch compact name="use_default_channels" .checked=${i} @change=${s=>{const n=s.currentTarget;n.closest(".channel-fields")?.querySelectorAll('input[data-default="true"]').forEach(o=>{o.disabled=n.checked,o.checked=n.checked||o.dataset.explicit==="true"}),n.form?.dispatchEvent(new Event("input",{bubbles:!0}))}}>Use default channels</upgrid-toggle-switch>
      <div class="channel-options">
        ${t.length?t.map(s=>{const n=e.includes(s.id),r=i&&s.default;return d`
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
                `}):d`<upgrid-empty-state>No notification channels are available</upgrid-empty-state>`}
      </div>
    </div>`}function dr(t,e=null,i=null,a=null){const s=n=>d`
    <option value="">No secret configured</option>
    ${t.map(r=>d`<option value=${r.id} ?selected=${r.id===n}>${r.name}</option>`)}
  `;return d`
    <fieldset class="tls-fields">
      <legend>HTTPS trust and mutual TLS</legend>
      <label>Custom CA bundle secret<select name="tls_ca_secret_id">${s(e)}</select></label>
      <div class="row">
        <label>Client certificate secret<select name="tls_client_certificate_secret_id">${s(i)}</select></label>
        <label>Client private key secret<select name="tls_client_private_key_secret_id">${s(a)}</select></label>
      </div>
      <p class="meta">PEM values stay encrypted. Client certificate and private key must be configured together.</p>
    </fieldset>
  `}const Pi={http:"https://example.com/health",tcp:"database.internal:5432",dns:"service.internal",icmp:"192.0.2.10",tls:"example.com:443"};function cr(t){return(t?.accepted_statuses??[{start:200,end:299}]).map(e=>e.start===e.end?e.start:`${e.start}-${e.end}`).join(",")}function ur(t){const e=t.currentTarget,i=e.form?.elements.namedItem("max_redirects");i&&(i.disabled=!e.checked),e.form?.dispatchEvent(new Event("input",{bubbles:!0}))}function Di({secrets:t,target:e,kindChanged:i}){const a=e?.kind??"http";if(e?.kind==="node")return d`
      <label>Name<input name="name" .value=${e.name} required /></label>
      <label>RPC URL<input .value=${e.url} disabled /></label>
    `;const s=a==="http",n=e?.follow_redirects??!0;return d`
    <label>Name<input name="name" placeholder="Production API" .value=${e?.name??""} required ?autofocus=${!e} /></label>
    <div class="row endpoint-row">
      <label>Type
        ${e?d`<input .value=${a.toUpperCase()} disabled />`:d`<select name="kind" @change=${i}><option value="http">HTTP</option><option value="tcp">TCP connect</option><option value="dns">DNS resolution</option><option value="icmp">ICMP echo</option><option value="tls">TLS certificate</option></select>`}
      </label>
      <label>URL / endpoint<input name="url" type=${s?"url":"text"} placeholder=${Pi[a]} .value=${e?.url??""} required /></label>
    </div>
    ${!e||s?d`
          <div class="http-fields" data-http-only ?hidden=${!s}>
            <div class="http-settings">
              <label>Method<input name="method" .value=${e?.method??"GET"} .defaultValue=${e?.method??"GET"} required /></label>
              <upgrid-toggle-switch compact name="follow_redirects" .checked=${n} @change=${ur}>Follow redirects</upgrid-toggle-switch>
              <label class="redirect-limit">Maximum redirects<input name="max_redirects" type="number" min="0" .value=${String(e?.max_redirects??5)} .defaultValue=${String(e?.max_redirects??5)} ?disabled=${!n} required /></label>
            </div>
            <upgrid-toggle-switch compact name="skip_tls_verification" .checked=${e?.skip_tls_verification??!1}>Skip TLS verification</upgrid-toggle-switch>
            ${dr(t,e?.tls_ca_secret_id,e?.tls_client_certificate_secret_id,e?.tls_client_private_key_secret_id)}
          </div>
        `:h}
  `}function Ii(t){const e=cr(t);return d`
    <upgrid-http-assertion-editor name="assertions" target-id=${t?.id??"new"} .assertions=${t?.assertions??[]}>
      <div slot="required" class="required-assertion">
        <label>Type<select aria-label="Status assertion type" disabled><option>Status code</option></select></label>
        <label>Expected status<input name="statuses" .value=${e} .defaultValue=${e} required /></label>
      </div>
    </upgrid-http-assertion-editor>
  `}function Oi(t){return t.closest("dialog")?.querySelector(".form-tabs")}function bt(t,e){Oi(t)?.querySelectorAll("[role='tab']").forEach(i=>{const a=i.dataset.tab===e;i.setAttribute("aria-selected",String(a)),i.tabIndex=-1}),t.querySelectorAll("[role='tabpanel']").forEach(i=>{i.hidden=i.dataset.panel!==e})}function pr(t){for(let e=0;e<t.elements.length;e+=1){const i=t.elements.item(e);if(i instanceof HTMLElement&&"checkValidity"in i&&typeof i.checkValidity=="function"&&!i.checkValidity())return i}}function ji(t,e){t.preventDefault();const i=t.currentTarget,a=pr(i);if(!a){e(t);return}const s=a.closest("[role='tabpanel']");s&&i.closest("dialog")?.querySelector(`[role='tab'][aria-controls='${s.id}']`)?.click(),queueMicrotask(()=>a.reportValidity())}function Li(t,e){const i=t.elements.namedItem("url");i&&(i.placeholder=Pi[e],i.type=e==="http"?"url":"text");const a=e!=="http";t.querySelectorAll("[data-http-only]").forEach(c=>{c.hidden=a,c.querySelectorAll("input, select, textarea, upgrid-http-assertion-editor, upgrid-toggle-switch").forEach(p=>{p.toggleAttribute("disabled",a)})});const s=t.elements.namedItem("follow_redirects"),n=t.elements.namedItem("max_redirects");n&&(n.disabled=a||!s?.checked);const r=Oi(t),o=r?.querySelector("[role='tab'][aria-selected='true']")?.dataset.tab??"general",l=r?.querySelector("[data-tab='assertions']");l&&(l.disabled=a),bt(t,a&&o==="assertions"?"general":o)}function hr(t){const e=t.currentTarget;e.form&&Li(e.form,e.value)}function Ce(t){const e=t.currentTarget;e.form&&e.dataset.tab&&bt(e.form,e.dataset.tab)}function gr(t){const e=t.currentTarget;queueMicrotask(()=>{Li(e,"http"),bt(e,"general")})}function mr(t,e,i,a,s){return d`
    <dialog id="target-dialog" aria-labelledby="add-target-title" @click=${s.backdrop}>
      <div class="dialog-head target-dialog-head">
        <h2 id="add-target-title">Add target</h2>
        <div class="form-tabs" role="tablist" aria-label="Target settings">
          <button id="target-general-tab" form="target-form" type="button" role="tab" data-tab="general" aria-controls="target-general-panel" aria-selected="true" tabindex="-1" @click=${Ce}>General</button>
          <button id="target-assertions-tab" form="target-form" type="button" role="tab" data-tab="assertions" aria-controls="target-assertions-panel" aria-selected="false" tabindex="-1" @click=${Ce}>Assertions</button>
          <button id="target-evaluation-tab" form="target-form" type="button" role="tab" data-tab="evaluation" aria-controls="target-evaluation-panel" aria-selected="false" tabindex="-1" @click=${Ce}>Evaluation</button>
          <button id="target-notifications-tab" form="target-form" type="button" role="tab" data-tab="notifications" aria-controls="target-notifications-panel" aria-selected="false" tabindex="-1" @click=${Ce}>Notifications</button>
        </div>
      </div>
      <form id="target-form" novalidate @submit=${n=>ji(n,s.create)} @input=${s.changed} @reset=${gr}>
        <section id="target-general-panel" class="target-tab-panel" role="tabpanel" data-panel="general" aria-labelledby="target-general-tab">
          ${Di({secrets:e,kindChanged:hr})}
        </section>
        <section id="target-assertions-panel" class="target-tab-panel" role="tabpanel" data-panel="assertions" data-http-only aria-labelledby="target-assertions-tab" hidden>
          ${Ii()}
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
          ${Ei(t)}
        </section>
        ${a?d`<div class="notice" role="alert">${a}</div>`:h}
        <div class="dialog-actions">
          <button class="button secondary" type="button" @click=${s.close}>Cancel</button>
          ${E({label:i?"Creating...":"Create target",busy:i,error:a})}
        </div>
      </form>
    </dialog>`}function fr(t,e,i,a,s,n,r,o,l,c,p){const u=t.kind==="node",f=t.kind==="http",m=t.history.slice(0,30).reverse(),$=Math.max(1,...m.map(v=>v.latency_ms)),S=e?.items??[],_=S.reduce((v,U)=>v+U.samples,0),B=S.reduce((v,U)=>v+U.successes,0),N=S.reduce((v,U)=>v+U.latency_total_ms,0),D=_?`${(B/_*100).toFixed(2)}%`:"—",T=new Map(r.map(v=>[v.id,v.name])),ne=v=>new Date(v).toLocaleString(void 0,{month:"short",day:"numeric",hour:"2-digit",minute:"2-digit"}),k=v=>v>=1e3?`${(v/1e3).toFixed(v>=1e4?0:1)} s`:`${Math.round(v)} ms`,C=_?k(N/_):"—",V=[{id:"details",label:"Details"},{id:"general",label:"General"},...f?[{id:"assertions",label:"Assertions"}]:[],...u?[]:[{id:"evaluation",label:"Evaluation"},{id:"notifications",label:"Notifications"}]],O=V.some(({id:v})=>v===n)?n:"details";return d`
    <dialog id="detail-dialog" aria-labelledby="target-detail-title" @click=${p.backdrop}>
      <div class="dialog-head target-dialog-head detail-dialog-head">
        <h2 id="target-detail-title">${u?"Node details":"Target details"}</h2>
        <div class="form-tabs" role="tablist" aria-label=${`${u?"Node":"Target"} details`}>
          ${V.map(({id:v,label:U})=>d`<button form="detail-form" type="button" role="tab" aria-controls=${`target-${v}-panel`} aria-selected=${String(O===v)} tabindex="-1" @click=${()=>p.selectTab(v)}>${U}</button>`)}
        </div>
        <upgrid-icon-button class="dialog-close" .icon=${ni} label=${`Close ${u?"Node":"Target"} details`} title="Close" @click=${p.close}></upgrid-icon-button>
      </div>
      <form id="detail-form" class="detail-form" novalidate @submit=${v=>ji(v,p.update)} @input=${p.changed}>
        <section id="target-details-panel" class="target-tab-panel details-panel" role="tabpanel" aria-label="Details" ?hidden=${O!=="details"}>
          <section class="history">
            <div class="history-head"><h3>Long-term summary</h3><span class="meta">Last 30 days</span></div>
            ${i?d`<p class="meta">Loading long-term history…</p>`:_?d`
                    <div class="history-summary" aria-label="Long-term evaluation summary">
                      <div><span>Availability</span><strong>${D}</strong></div>
                      <div><span>Average latency</span><strong>${C}</strong></div>
                      <div><span>Evaluations</span><strong>${_.toLocaleString()}</strong></div>
                    </div>
                  `:d`<upgrid-empty-state>No long-term history recorded yet</upgrid-empty-state>`}
          </section>
          <section class="history">
            <div class="history-head"><h3>Evaluation history</h3>${m.length?d`<span class="meta">Latest ${m.length}</span>`:h}</div>
            ${m.length?d`
              <div class="chart-plot">
                <div class="chart-scale" aria-hidden="true"><span>${k($)}</span><span>${k($/2)}</span><span>0 ms</span></div>
                <div class="history-chart" role="list" aria-label=${`Recent evaluation latency, 0 to ${k($)}`}>
                  ${m.map(v=>{const U=v.succeeded?"Passed":"Failed",Mi=u||!f?v.succeeded?"reachable":"unreachable":v.status_code===null?"network error":`HTTP ${v.status_code}`,Ri=T.get(v.executor_node_id)??`Node ${v.executor_node_id.slice(0,8)}`,vt=`${U} at ${new Date(v.recorded_at_ms).toLocaleString()}: ${v.latency_ms} ms, ${Mi}. Executed by ${Ri}`;return d`<span class="history-bar ${v.succeeded?"up":"down"}" role="listitem" aria-label=${vt} title=${vt} style=${`height: ${Math.max(8,v.latency_ms/$*100)}%`}></span>`})}
                </div>
              </div>
              <div class="chart-axis"><span>${ne(m[0].recorded_at_ms)}</span><span>${ne(m[m.length-1].recorded_at_ms)}</span></div>
              <div class="chart-legend"><span><i class="up"></i>Passed</span><span><i class="down"></i>Failed</span><span>Height = latency</span></div>
            `:d`<upgrid-empty-state>No evaluations recorded yet</upgrid-empty-state>`}
          </section>
        </section>
        <section id="target-general-panel" class="target-tab-panel" role="tabpanel" aria-label="General" ?hidden=${O!=="general"}>
          ${Di({secrets:l,target:t})}
        </section>
        ${f?d`<section id="target-assertions-panel" class="target-tab-panel" role="tabpanel" aria-label="Assertions" ?hidden=${O!=="assertions"}>
                ${Ii(t)}
              </section>`:h}
        ${u?h:d`
              <section id="target-evaluation-panel" class="target-tab-panel" role="tabpanel" aria-label="Evaluation" ?hidden=${O!=="evaluation"}>
                <div class="row"><label>Interval (seconds)<input name="interval" type="number" min="1" .value=${String(t.interval_seconds)} required /></label><label>Timeout (seconds)<input name="timeout" type="number" min="1" .value=${String(t.timeout_seconds)} required /></label></div>
                <div class="row"><label>Failures before down<input name="failures" type="number" min="1" .value=${String(t.failure_threshold)} required /></label><label>Evaluation locations<input name="locations" type="number" min="1" max="32" .value=${String(t.locations)} required /></label></div>
              </section>
              <section id="target-notifications-panel" class="target-tab-panel" role="tabpanel" aria-label="Notifications" ?hidden=${O!=="notifications"}>
                ${Ei(o,t.notification_channel_ids,t.use_default_channels)}
              </section>
            `}
        ${c?d`<div class="notice" role="alert">${c}</div>`:h}
        ${O==="details"?u?h:d`<div class="dialog-actions"><div class="danger-actions">
                  <upgrid-icon-button .icon=${de} label="Move target to trash" title="Move to trash" variant="danger" @click=${p.delete}></upgrid-icon-button>
                  <upgrid-icon-button .icon=${t.paused?ri:ai} label=${t.paused?"Resume evaluations":"Pause evaluations"} title=${t.paused?"Resume evaluations":"Pause evaluations"} .variant=${t.paused?"success":"warning"} @click=${()=>p.pause(!t.paused)}></upgrid-icon-button>
                </div></div>`:d`<div class="dialog-actions">${E({label:"Save changes",busy:a,changed:s,error:c,baselineKey:t.id})}</div>`}
      </form>
    </dialog>`}var br=Object.getOwnPropertyDescriptor,vr=(t,e,i,a)=>{for(var s=a>1?void 0:a?br(e,i):e,n=t.length-1,r;n>=0;n--)(r=t[n])&&(s=r(s)||s);return s};let nt=class extends Ya{renderBrand(){return Ci(this.live,t=>this.navigate(t,"overview"))}render(){const t=this.targets.filter(r=>r.availability==="up").length,e=this.targets.filter(r=>r.availability==="down").length,i=this.alerts.filter(r=>r.delivery==="pending").length,a=["overview","alerts","cluster","trash"],s=this.targets.filter(r=>`${r.name} ${r.url}`.toLowerCase().includes(this.search.toLowerCase())).filter(r=>this.statusFilter==="all"?!0:this.statusFilter==="paused"?r.paused:r.availability===this.statusFilter).sort((r,o)=>this.sort==="status"&&r.availability.localeCompare(o.availability)||r.name.localeCompare(o.name)),n={login:r=>{this.login(r)},logout:()=>{this.logout()},createIdentity:r=>{this.createIdentity(r)},openAddUser:()=>this.showDialog("add-user-dialog"),closeAddUser:()=>this.closeDialog("add-user-dialog"),openEditUser:r=>{this.editingIdentity=r,this.updateComplete.then(()=>this.showDialog("edit-user-dialog"))},closeEditUser:()=>{this.closeDialog("edit-user-dialog"),this.editingIdentity=void 0},openApiToken:()=>this.showDialog("api-token-dialog"),closeApiToken:()=>this.closeDialog("api-token-dialog"),dismissDialog:r=>this.dismissOnBackdrop(r),updateIdentity:(r,o)=>{this.updateIdentity(r,o)},deleteIdentity:r=>{this.deleteIdentity(r)},createApiToken:r=>{this.createApiToken(r)},revokeApiToken:r=>{this.revokeApiToken(r)},dismissToken:()=>this.newApiToken="",changed:()=>this.error=""};return this.authReady&&!this.setupMode&&!this.session&&!this.publicStatus?d`${ir(this.live,this.saving,this.error,n)}${Te()}`:this.setupMode&&this.setup?d`
        <main class="shell setup-shell">
          <header>
            ${this.renderBrand()}
            <div></div>
            <div class="actions"><upgrid-icon-button .icon=${Qe[this.theme]} label=${`Theme: ${this.theme}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}></upgrid-icon-button></div>
          </header>
          ${this.error?d`<div class="notice" role="alert">${this.error}</div>`:h}
          <upgrid-setup .setup=${this.setup} @setup-changed=${this.setupChanged}></upgrid-setup>
        </main>${Te()}`:!this.session&&this.publicStatus?this.renderPublicStatusPage(this.publicStatus.targets):d`
      <main class="shell">
        <header>
          ${this.renderBrand()}
          <nav aria-label="Primary">
            ${a.map(r=>d`<a class=${this.activeSection===r?"active":""} href=${Y[r]} @click=${o=>this.navigate(o,r)}>${r[0].toUpperCase()}${r.slice(1)}</a>`)}
          </nav>
          <div class="actions">
            <upgrid-icon-button .icon=${Qe[this.theme]} label=${`Theme: ${this.theme}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}></upgrid-icon-button>
            <details class="account-menu">
              <summary class="button secondary account-menu-trigger" aria-label=${`Account menu for ${this.session?.username}`} title=${`Account: ${this.session?.username}`}><iconify-icon .icon=${ds} aria-hidden="true"></iconify-icon></summary>
              <div class="account-dropdown" role="menu">
                <a class="button secondary" role="menuitem" href=${Y.manage} @click=${r=>this.navigate(r,"manage")}>Manage</a>
                <a class="button secondary" role="menuitem" href=${Y.changePassword} @click=${r=>this.navigate(r,"changePassword")}>Change password</a>
                <a class="button secondary" role="menuitem" href=${Y.users} @click=${r=>this.navigate(r,"users")}>Manage user</a>
                <a class="button secondary" role="menuitem" href=${Y.apiTokens} @click=${r=>this.navigate(r,"apiTokens")}>API token</a>
                <div class="account-separator" role="separator"></div>
                <button class="button danger" role="menuitem" type="button" @click=${()=>{this.logout()}}>Logout</button>
              </div>
            </details>
          </div>
        </header>
        ${this.error?d`<div class="notice" role="alert">${this.error}</div>`:h}
        ${this.setup?.warning&&!this.warningDismissed?d`<div class="notice" role="status">${this.setup.warning}<button class="button secondary" style="float: right; margin: -6px" @click=${this.dismissWarning}>Dismiss</button></div>`:h}
        ${this.activeSection==="overview"?this.renderOverview(s,t,e,i):this.activeSection==="alerts"?tr(this.alerts,this.transitions,this.channels,{search:this.alertSearch,delivery:this.alertDeliveryFilter,kind:this.alertKindFilter,acknowledged:this.alertAcknowledgedFilter},this.saving,{create:()=>this.openChannelDialog(),edit:r=>this.openChannelDialog(r),remove:r=>{this.deleteResource("channels",r.id,r.name)},setDefault:(r,o)=>{this.setChannelDefault(r,o)},acknowledge:r=>{this.acknowledgeAlert(r)},retry:r=>{this.retryAlert(r)},setSearch:r=>this.alertSearch=r,setDelivery:r=>this.alertDeliveryFilter=r,setKind:r=>this.alertKindFilter=r,setAcknowledged:r=>this.alertAcknowledgedFilter=r}):this.activeSection==="cluster"?this.renderClusterPage():this.activeSection==="trash"?this.renderTrashPage():this.activeSection==="manage"?nr(this.settings,this.saving,this.error,r=>{this.updateSettings(r)},()=>this.error=""):this.activeSection==="changePassword"?sr(this.identities.find(r=>r.id===this.session?.identity_id),this.saving,this.error,n):this.activeSection==="users"?ar(this.identities,this.session?.identity_id,this.editingIdentity,this.saving,this.error,n):rr(this.apiTokens,this.newApiToken,this.saving,this.error,n)}
      </main>${Te()}
      ${mr(this.channels,this.secrets,this.saving,this.targetError,{backdrop:r=>this.dismissOnBackdrop(r),close:()=>this.closeTargetDialog(),create:r=>{this.createTarget(r)},changed:()=>this.targetError=""})}
      ${this.selected?fr(this.selected,this.targetHistory,this.historyLoading,this.saving,this.detailDirty,this.detailTab,this.cluster?.members??[],this.channels,this.secrets,this.targetError,{backdrop:r=>this.dismissOnBackdrop(r),close:()=>this.closeDetailDialog(),update:r=>{this.updateTarget(r)},changed:r=>this.updateDetailDirty(r),delete:()=>{this.deleteTarget()},selectTab:r=>this.selectDetailTab(r),pause:r=>{this.setPaused(r)}}):h}
      <dialog id="secret-dialog" aria-labelledby="secret-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><div class="title-with-help"><h2 id="secret-title">Add secret</h2>${X("add-secret-help","About adding a secret","Create an encrypted, write-only secret to reference from supported field like TLS key.")}</div></div>
        <form @submit=${this.createSecret} @input=${()=>this.error=""}>
          <label>Name<input name="name" placeholder="Webhook token" required autofocus /></label>
          <label>Value<input name="value" type="password" autocomplete="new-password" required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("secret-dialog")}>Cancel</button>${E({label:"Create secret",busy:this.saving,error:this.error})}</div>
        </form>
      </dialog>
      <dialog id="channel-dialog" aria-labelledby="channel-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="channel-title">${this.editingChannel?"Edit channel":"Add channel"}</h2></div>
        <upgrid-notification-channel-form
          .channel=${this.editingChannel}
          .submitLabel=${this.editingChannel?"Save changes":"Create channel"}
          cancel-label="Cancel"
          @channel-saved=${this.channelSaved}
          @channel-cancel=${()=>{this.editingChannel=void 0,this.closeDialog("channel-dialog")}}
        ></upgrid-notification-channel-form>
      </dialog>
      <dialog id="token-config-dialog" aria-labelledby="token-config-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><div class="title-with-help"><h2 id="token-config-title">Create join token</h2>${X("join-token-config-help","About join token settings","Choose how many days the token remains valid and whether it can be reused.")}</div></div>
        <form @submit=${this.createJoinToken} @input=${()=>this.error=""}>
          <label>Expiration (days)<input name="expiration_days" type="number" min="1" step="1" value="1" required autofocus /></label>
          <upgrid-toggle-switch .checked=${this.unlimitedUses} @change=${r=>this.unlimitedUses=r.currentTarget.checked}>Unlimited uses</upgrid-toggle-switch>
          <label>Maximum uses<input name="max_uses" type="number" min="1" step="1" value="1" ?disabled=${this.unlimitedUses} required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("token-config-dialog")}>Cancel</button>${E({label:this.saving?"Creating...":"Create token",busy:this.saving,error:this.error})}</div>
        </form>
      </dialog>
      <dialog id="join-dialog" aria-labelledby="join-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><div class="title-with-help"><h2 id="join-title">Join token created</h2>${X("join-token-url-help","About join token credentials","This URL contains cluster credentials. Revoke the token when no longer needed.")}</div></div>
        <div class="join-url">${this.joinUrl}</div>
        <div class="dialog-actions" style="padding: 0 22px 22px"><button class="button secondary" autofocus @click=${()=>this.closeDialog("join-dialog")}>Close</button><button class="button" @click=${this.copyJoinUrl}>${this.copied?"Copied":"Copy URL"}</button></div>
      </dialog>
    `}renderPublicStatusPage(t){const e=t.filter(s=>s.availability==="up"&&!s.paused).length,i=t.filter(s=>s.availability==="down"&&!s.paused).length,a=t.filter(s=>s.paused).length;return d`
      <main class="shell">
        <header>
          ${this.renderBrand()}
          <nav aria-label="Primary"><a class="active" href="/">Status</a></nav>
          <div class="actions">
            <upgrid-icon-button .icon=${Qe[this.theme]} label=${`Theme: ${this.theme}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}></upgrid-icon-button>
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
        ${A({title:"Targets",label:"Public target status",metadata:`${t.length} monitored`,className:"public-status-card",content:d`
            ${t.length?t.map(s=>{const n=s.latest_evaluation,r=s.paused?"paused":s.availability==="down"?"down":s.consecutive_failures>0?"suspicious":s.availability,o=s.paused?"Paused":n?`${n.latency_ms} ms · ${n.status_code??(n.succeeded?"reachable":"unreachable")}`:"Waiting for an evaluation";return d`<div class="resource"><div><strong>${s.name}</strong><code>${s.kind.toUpperCase()} · ${o}</code></div><span class=${`badge ${r}`}>${r}</span></div>`}):d`<upgrid-empty-state>No targets are configured</upgrid-empty-state>`}
          `})}
      </main>${Te()}`}renderOverview(t,e,i,a){const s=this.targets.filter(l=>this.selectedIds.has(l.id)),n=s.some(l=>!l.paused),r=s.some(l=>l.paused),o=this.secrets.filter(l=>!l.referenced);return d`
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
        ${A({title:"Secrets",tooltip:{id:"secrets-help",label:"About reusable secrets",message:"Reusable secrets are encrypted and write-only. Reference them from supported fields like TLS key. In use ones cannot be deleted."},actions:[...o.length?[{key:"delete-unused",label:`Delete unused (${o.length})`,variant:"danger",disabled:this.saving,onClick:()=>this.cleanupSecrets()}]:[],{key:"add-secret",label:"Add secret",variant:"secondary",onClick:()=>this.showDialog("secret-dialog")}],content:d`
            ${this.secrets.length?this.secrets.map(l=>d`<div class="resource"><div><strong>${l.name}</strong><code>${l.id} · ${l.referenced?"In use":"Unused"}</code></div><upgrid-icon-button .icon=${de} label=${`Delete secret ${l.name}`} title=${l.referenced?"Secret is in use":`Delete ${l.name}`} variant="danger" ?disabled=${l.referenced||this.saving} @click=${()=>this.deleteResource("secrets",l.id,l.name)}></upgrid-icon-button></div>`):d`<upgrid-empty-state>No reusable secrets</upgrid-empty-state>`}
          `})}
      </section>
      ${A({title:"Targets",metadata:`${this.targets.length} configured`,content:d`
          <div class="toolbar">
            <input aria-label="Search targets" type="search" placeholder="Search name or URL" .value=${this.search} @input=${l=>this.search=l.target.value} />
            <select aria-label="Filter targets" .value=${this.statusFilter} @change=${l=>this.statusFilter=l.target.value}><option value="all">All states</option><option value="up">Up</option><option value="down">Down</option><option value="unknown">Unknown</option><option value="paused">Paused</option></select>
            <select aria-label="Sort targets" .value=${this.sort} @change=${l=>this.sort=l.target.value}><option value="name">Sort by name</option><option value="status">Sort by status</option></select>
          </div>
          ${this.selectedIds.size?d`<div class="bulk"><span class="meta">${this.selectedIds.size} selected</span><div class="bulk-actions"><upgrid-icon-button .icon=${ni} label="Unselect all" title="Unselect all" @click=${()=>this.selectedIds=new Set}></upgrid-icon-button>${n?d`<upgrid-icon-button .icon=${ai} label="Pause selected" title="Pause selected" variant="warning" @click=${()=>this.bulkPause(!0)}></upgrid-icon-button>`:h}${r?d`<upgrid-icon-button .icon=${ri} label="Resume selected" title="Resume selected" variant="success" @click=${()=>this.bulkPause(!1)}></upgrid-icon-button>`:h}<upgrid-icon-button .icon=${de} label="Delete selected" title="Delete selected" variant="danger" @click=${this.bulkDelete}></upgrid-icon-button></div></div>`:h}
          ${t.length?t.map(l=>this.renderTarget(l)):d`<upgrid-empty-state>${this.targets.length?"No targets match these filters":"No targets yet. Add the first one to begin monitoring"}</upgrid-empty-state>`}
        `})}
    `}renderTrashPage(){return d`
      <section class="heading" id="trash">
        <div><span class="eyebrow">Recover deleted monitors</span><h1>Trash</h1></div>
      </section>
      ${A({title:"Deleted targets",label:"Trashed targets",tooltip:{id:"trash-retention-help",label:"About deleted target retention",message:"Settings and history remain recoverable until the retention deadline."},metadata:`${this.trashedTargets.length} stored`,content:d`${this.trashedTargets.length?this.trashedTargets.map(t=>this.renderTrashedTarget(t)):d`<upgrid-empty-state>Trash is empty</upgrid-empty-state>`}`})}
    `}renderTrashedTarget(t){return d`
      <div class="resource">
        <div>
          <strong>${t.name}</strong>
          <code>${t.kind.toUpperCase()} · trashed ${new Date(t.deleted_at_ms).toLocaleString()} · delete on ${new Date(t.purge_at_ms).toLocaleString()}</code>
        </div>
        <div class="actions">
          <button class="button secondary" ?disabled=${this.saving} @click=${()=>this.restoreTarget(t)}>Restore</button>
          <button class="button danger" ?disabled=${this.saving} @click=${()=>this.purgeTarget(t)}>Delete permanently</button>
        </div>
      </div>
    `}renderClusterMember(t){return d`
      <div class="resource">
        <div>
          <strong>${t.name}</strong>
          <code>${t.raft_url} · ${t.active_assignments} active assignments</code>
        </div>
        <div class="actions">
          ${t.local?d`<span class="badge">This node</span>`:h}
          ${t.leader?d`<span class="badge">Leader</span>`:h}
          ${t.draining?d`<span class="badge">Draining</span>`:h}
          ${t.local?h:d`
                <button class="button secondary" ?disabled=${this.saving} @click=${()=>this.setNodeDrain(t,!t.draining)}>${t.draining?"Cancel drain":"Drain"}</button>
                ${t.draining&&t.active_assignments===0?d`<button class="button danger" ?disabled=${this.saving} @click=${()=>this.removeNode(t,!1)}>Remove</button>`:h}
                <button class="button danger" ?disabled=${this.saving} @click=${()=>this.removeNode(t,!0)}>Replace failed</button>
              `}
        </div>
      </div>
    `}renderClusterPage(){return d`
      <section class="heading" id="cluster">
        <div><span class="eyebrow">Raft membership</span><h1>Cluster</h1></div>
        <div class="actions">
          <button class="button" @click=${this.openTokenDialog}>Create token</button>
        </div>
      </section>
      <div class="page-columns">
      ${A({title:"Nodes",label:"Cluster topology",tooltip:{id:"nodes-removal-help",label:"About removing nodes",message:"Drain healthy nodes before removal. Replace failed nodes only after confirming the old process is permanently stopped."},metadata:`${this.cluster?.members.length??0} members`,content:d`
          ${this.cluster?.members.map(t=>this.renderClusterMember(t))}
          ${this.cluster?.members.length?h:d`<upgrid-empty-state>Cluster topology unavailable</upgrid-empty-state>`}
        `})}
      ${A({title:"Join tokens",metadata:`${this.joinTokens.length} stored`,content:d`
          ${this.joinTokens.length?this.joinTokens.map(t=>d`
                    <div class="resource">
                      <div><strong>${t.id.slice(0,12)}…</strong><code>Expires ${new Date(t.expires_at_ms).toLocaleString()} · ${t.remaining_uses===null?"unlimited uses":`${t.remaining_uses} uses left`}</code></div>
                      <button class="button danger" aria-label=${`Revoke join token ${t.id.slice(0,12)}`} @click=${()=>this.revokeJoinToken(t)}>Revoke</button>
                    </div>
                  `):d`<upgrid-empty-state>No join tokens</upgrid-empty-state>`}
        `})}
      </div>
    `}renderTarget(t){const e=t.kind==="node",i=t.kind==="http",a=t.latest_evaluation,s=t.history.slice(0,16).reverse(),n=Math.max(1,...s.map(o=>o.latency_ms)),r=t.paused?"paused":t.availability==="down"?"down":t.consecutive_failures>0?"suspicious":t.availability;return d`
      <div class="target-wrap">
        ${e?d`<input class="select-target" type="checkbox" aria-label=${`Select ${t.name}`} disabled />`:d`<input class="select-target" type="checkbox" aria-label=${`Select ${t.name}`} .checked=${this.selectedIds.has(t.id)} @change=${o=>this.toggleSelected(t.id,o.target.checked)} />`}
        <button class=${`target ${e?"node-target":""}`} aria-label=${t.name} @click=${()=>this.openTarget(t)}>
          <i class="state ${r}" aria-label=${r}></i>
          <div>
            <div class="target-title"><h3>${t.name}</h3><span class="badge">${e?"Node":t.kind.toUpperCase()}</span></div>
            <div class="meta">${t.paused?"Paused · ":""}${i||e?`${t.method} · `:""}${t.url} · every ${t.interval_seconds}s${e?"":` · ${t.locations} ${t.locations===1?"location":"locations"}`}</div>
          </div>
          <div class="target-side">
            ${s.length?d`<div class="mini-chart" aria-hidden="true">${s.map(o=>d`<i class="mini-bar ${o.succeeded?"up":"down"}" style=${`height: ${Math.max(12,o.latency_ms/n*100)}%`}></i>`)}</div>`:h}
            <div class="latency">
              <strong>${a?`${a.latency_ms} ms`:"—"}</strong>
              <span>${a?i?a.status_code??"network error":a.succeeded?"reachable":"unreachable":"waiting"}</span>
            </div>
          </div>
        </button>
      </div>
    `}};nt.styles=R`
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
    .account-menu-trigger { display: grid; width: 44px; height: 44px; min-height: 44px; place-items: center; padding: 0; }
    iconify-icon { display: inline-block; width: 18px; height: 18px; font-size: 18px; }
    .account-menu { position: relative; }
    .account-menu summary { list-style: none; }
    .account-dropdown { position: absolute; top: calc(100% + 8px); right: 0; z-index: 20; display: grid; width: max-content; min-width: 180px; gap: 2px; border: 1px solid var(--line); border-radius: 14px; background: var(--panel); padding: 6px; box-shadow: 0 16px 40px var(--dialog-shadow); }
    .account-dropdown .button { display: flex; width: 100%; min-height: 44px; align-items: center; justify-content: flex-start; box-sizing: border-box; border: 0; border-radius: 10px; background: transparent; padding: 9px 13px; color: var(--muted); font: inherit; line-height: 1.2; text-align: left; text-decoration: none; }
    .account-dropdown .button:hover, .account-dropdown .button:focus-visible { background: var(--row-hover); color: var(--text); }
    .account-separator { height: 1px; margin: 4px 0; background: var(--divider); }
    .account-dropdown .danger { color: var(--danger-text); }
    .account-dropdown .danger:hover, .account-dropdown .danger:focus-visible { background: var(--notice-bg); color: var(--danger-text); }
    ${ft}
    ${Ti}
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
    .channel-actions upgrid-toggle-switch { font-size: 12px; }
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
    #target-dialog, #detail-dialog { width: min(720px, calc(100% - 28px)); }
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
    .form-tabs { display: flex; width: fit-content; min-width: 0; max-width: 100%; gap: 4px; border: 1px solid var(--line); border-radius: 14px; background: var(--nav-bg); padding: 4px; overflow-x: auto; scrollbar-width: none; }
    .form-tabs::-webkit-scrollbar { display: none; }
    .form-tabs button { min-height: 34px; border: 0; border-radius: 10px; background: transparent; color: var(--muted); padding: 7px 11px; white-space: nowrap; cursor: pointer; transition: background-color 160ms ease, color 160ms ease; }
    .form-tabs button:hover { background: transparent; color: var(--muted); }
    .form-tabs button[aria-selected="true"], .form-tabs button[aria-selected="true"]:hover { background: var(--active-bg); color: var(--text); }
    .form-tabs button:disabled, .form-tabs button:disabled:hover { background: transparent; color: var(--disabled-text); cursor: not-allowed; opacity: 1; }
    .target-tab-panel { display: grid; gap: 13px; min-height: 190px; align-content: start; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    .endpoint-row { grid-template-columns: minmax(140px, 1fr) minmax(0, 2fr); }
    .http-settings { display: grid; grid-template-columns: minmax(140px, 1fr) auto auto; gap: 11px; align-items: end; }
    .http-settings > upgrid-toggle-switch { align-self: end; margin-bottom: 10px; }
    .redirect-limit { display: flex; align-items: center; gap: 10px; white-space: nowrap; }
    .redirect-limit input { width: 72px; }
    .http-fields { display: grid; gap: 13px; }
    .http-fields .tls-fields { margin-top: 0; }
    label { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
    [hidden] { display: none !important; }
    input, select { width: 100%; min-height: 44px; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; font-size: 16px; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease; }
    input:focus, select:focus { border-color: var(--focus); }
    button:focus-visible, a:focus-visible, .target:focus-visible, input:focus-visible, select:focus-visible { outline: 2px solid var(--green); outline-offset: 2px; }
    button, a, summary, [role="button"], [role="tab"], input[type="checkbox"], input[type="radio"], select, .target, .checkbox-option { cursor: pointer; user-select: none; }
    button:disabled { border-color: var(--disabled-border); background: var(--disabled-bg); color: var(--disabled-text); cursor: not-allowed; opacity: 1; }
    input:disabled, select:disabled { cursor: not-allowed; }
    input:disabled { cursor: not-allowed; opacity: .5; }
    .dialog-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 5px; }
    .danger-actions { display: flex; gap: 8px; margin-left: auto; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .danger { background: transparent; color: var(--danger-text); border-color: var(--danger-border); }
    .danger:hover:not(:disabled) { border-color: var(--danger-text); }
    .dialog-close { position: absolute; top: 12px; right: 14px; --icon-button-radius: 14px; }
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
    @media (max-width: 600px) {
      .http-settings { grid-template-columns: minmax(0, 1fr); }
      .http-settings > upgrid-toggle-switch { margin-bottom: 0; }
      .redirect-limit { justify-content: space-between; }
    }
    @media (max-width: 480px) {
      .row, .endpoint-row { grid-template-columns: minmax(0, 1fr); }
    }
  `;nt=vr([z("upgrid-app")],nt);
