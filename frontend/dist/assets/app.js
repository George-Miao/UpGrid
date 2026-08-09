(function(){const t=document.createElement("link").relList;if(t&&t.supports&&t.supports("modulepreload"))return;for(const s of document.querySelectorAll('link[rel="modulepreload"]'))n(s);new MutationObserver(s=>{for(const r of s)if(r.type==="childList")for(const o of r.addedNodes)o.tagName==="LINK"&&o.rel==="modulepreload"&&n(o)}).observe(document,{childList:!0,subtree:!0});function i(s){const r={};return s.integrity&&(r.integrity=s.integrity),s.referrerPolicy&&(r.referrerPolicy=s.referrerPolicy),s.crossOrigin==="use-credentials"?r.credentials="include":s.crossOrigin==="anonymous"?r.credentials="omit":r.credentials="same-origin",r}function n(s){if(s.ep)return;s.ep=!0;const r=i(s);fetch(s.href,r)}})();const ee=globalThis,Se=ee.ShadowRoot&&(ee.ShadyCSS===void 0||ee.ShadyCSS.nativeShadow)&&"adoptedStyleSheets"in Document.prototype&&"replace"in CSSStyleSheet.prototype,Ae=Symbol(),De=new WeakMap;let dt=class{constructor(t,i,n){if(this._$cssResult$=!0,n!==Ae)throw Error("CSSResult is not constructable. Use `unsafeCSS` or `css` instead.");this.cssText=t,this.t=i}get styleSheet(){let t=this.o;const i=this.t;if(Se&&t===void 0){const n=i!==void 0&&i.length===1;n&&(t=De.get(i)),t===void 0&&((this.o=t=new CSSStyleSheet).replaceSync(this.cssText),n&&De.set(i,t))}return t}toString(){return this.cssText}};const Ot=e=>new dt(typeof e=="string"?e:e+"",void 0,Ae),Dt=(e,...t)=>{const i=e.length===1?e[0]:t.reduce((n,s,r)=>n+(o=>{if(o._$cssResult$===!0)return o.cssText;if(typeof o=="number")return o;throw Error("Value passed to 'css' function must be a 'css' function result: "+o+". Use 'unsafeCSS' to pass non-literal values, but take care to ensure page security.")})(s)+e[r+1],e[0]);return new dt(i,e,Ae)},Mt=(e,t)=>{if(Se)e.adoptedStyleSheets=t.map(i=>i instanceof CSSStyleSheet?i:i.styleSheet);else for(const i of t){const n=document.createElement("style"),s=ee.litNonce;s!==void 0&&n.setAttribute("nonce",s),n.textContent=i.cssText,e.appendChild(n)}},Me=Se?e=>e:e=>e instanceof CSSStyleSheet?(t=>{let i="";for(const n of t.cssRules)i+=n.cssText;return Ot(i)})(e):e;const{is:Nt,defineProperty:Rt,getOwnPropertyDescriptor:Lt,getOwnPropertyNames:Ut,getOwnPropertySymbols:Ft,getPrototypeOf:Ht}=Object,ce=globalThis,Ne=ce.trustedTypes,qt=Ne?Ne.emptyScript:"",zt=ce.reactiveElementPolyfillSupport,q=(e,t)=>e,ne={toAttribute(e,t){switch(t){case Boolean:e=e?qt:null;break;case Object:case Array:e=e==null?e:JSON.stringify(e)}return e},fromAttribute(e,t){let i=e;switch(t){case Boolean:i=e!==null;break;case Number:i=e===null?null:Number(e);break;case Object:case Array:try{i=JSON.parse(e)}catch{i=null}}return i}},Ce=(e,t)=>!Nt(e,t),Re={attribute:!0,type:String,converter:ne,reflect:!1,useDefault:!1,hasChanged:Ce};Symbol.metadata??=Symbol("metadata"),ce.litPropertyMetadata??=new WeakMap;let D=class extends HTMLElement{static addInitializer(t){this._$Ei(),(this.l??=[]).push(t)}static get observedAttributes(){return this.finalize(),this._$Eh&&[...this._$Eh.keys()]}static createProperty(t,i=Re){if(i.state&&(i.attribute=!1),this._$Ei(),this.prototype.hasOwnProperty(t)&&((i=Object.create(i)).wrapped=!0),this.elementProperties.set(t,i),!i.noAccessor){const n=Symbol(),s=this.getPropertyDescriptor(t,n,i);s!==void 0&&Rt(this.prototype,t,s)}}static getPropertyDescriptor(t,i,n){const{get:s,set:r}=Lt(this.prototype,t)??{get(){return this[i]},set(o){this[i]=o}};return{get:s,set(o){const a=s?.call(this);r?.call(this,o),this.requestUpdate(t,a,n)},configurable:!0,enumerable:!0}}static getPropertyOptions(t){return this.elementProperties.get(t)??Re}static _$Ei(){if(this.hasOwnProperty(q("elementProperties")))return;const t=Ht(this);t.finalize(),t.l!==void 0&&(this.l=[...t.l]),this.elementProperties=new Map(t.elementProperties)}static finalize(){if(this.hasOwnProperty(q("finalized")))return;if(this.finalized=!0,this._$Ei(),this.hasOwnProperty(q("properties"))){const i=this.properties,n=[...Ut(i),...Ft(i)];for(const s of n)this.createProperty(s,i[s])}const t=this[Symbol.metadata];if(t!==null){const i=litPropertyMetadata.get(t);if(i!==void 0)for(const[n,s]of i)this.elementProperties.set(n,s)}this._$Eh=new Map;for(const[i,n]of this.elementProperties){const s=this._$Eu(i,n);s!==void 0&&this._$Eh.set(s,i)}this.elementStyles=this.finalizeStyles(this.styles)}static finalizeStyles(t){const i=[];if(Array.isArray(t)){const n=new Set(t.flat(1/0).reverse());for(const s of n)i.unshift(Me(s))}else t!==void 0&&i.push(Me(t));return i}static _$Eu(t,i){const n=i.attribute;return n===!1?void 0:typeof n=="string"?n:typeof t=="string"?t.toLowerCase():void 0}constructor(){super(),this._$Ep=void 0,this.isUpdatePending=!1,this.hasUpdated=!1,this._$Em=null,this._$Ev()}_$Ev(){this._$ES=new Promise(t=>this.enableUpdating=t),this._$AL=new Map,this._$E_(),this.requestUpdate(),this.constructor.l?.forEach(t=>t(this))}addController(t){(this._$EO??=new Set).add(t),this.renderRoot!==void 0&&this.isConnected&&t.hostConnected?.()}removeController(t){this._$EO?.delete(t)}_$E_(){const t=new Map,i=this.constructor.elementProperties;for(const n of i.keys())this.hasOwnProperty(n)&&(t.set(n,this[n]),delete this[n]);t.size>0&&(this._$Ep=t)}createRenderRoot(){const t=this.shadowRoot??this.attachShadow(this.constructor.shadowRootOptions);return Mt(t,this.constructor.elementStyles),t}connectedCallback(){this.renderRoot??=this.createRenderRoot(),this.enableUpdating(!0),this._$EO?.forEach(t=>t.hostConnected?.())}enableUpdating(t){}disconnectedCallback(){this._$EO?.forEach(t=>t.hostDisconnected?.())}attributeChangedCallback(t,i,n){this._$AK(t,n)}_$ET(t,i){const n=this.constructor.elementProperties.get(t),s=this.constructor._$Eu(t,n);if(s!==void 0&&n.reflect===!0){const r=(n.converter?.toAttribute!==void 0?n.converter:ne).toAttribute(i,n.type);this._$Em=t,r==null?this.removeAttribute(s):this.setAttribute(s,r),this._$Em=null}}_$AK(t,i){const n=this.constructor,s=n._$Eh.get(t);if(s!==void 0&&this._$Em!==s){const r=n.getPropertyOptions(s),o=typeof r.converter=="function"?{fromAttribute:r.converter}:r.converter?.fromAttribute!==void 0?r.converter:ne;this._$Em=s;const a=o.fromAttribute(i,r.type);this[s]=a??this._$Ej?.get(s)??a,this._$Em=null}}requestUpdate(t,i,n,s=!1,r){if(t!==void 0){const o=this.constructor;if(s===!1&&(r=this[t]),n??=o.getPropertyOptions(t),!((n.hasChanged??Ce)(r,i)||n.useDefault&&n.reflect&&r===this._$Ej?.get(t)&&!this.hasAttribute(o._$Eu(t,n))))return;this.C(t,i,n)}this.isUpdatePending===!1&&(this._$ES=this._$EP())}C(t,i,{useDefault:n,reflect:s,wrapped:r},o){n&&!(this._$Ej??=new Map).has(t)&&(this._$Ej.set(t,o??i??this[t]),r!==!0||o!==void 0)||(this._$AL.has(t)||(this.hasUpdated||n||(i=void 0),this._$AL.set(t,i)),s===!0&&this._$Em!==t&&(this._$Eq??=new Set).add(t))}async _$EP(){this.isUpdatePending=!0;try{await this._$ES}catch(i){Promise.reject(i)}const t=this.scheduleUpdate();return t!=null&&await t,!this.isUpdatePending}scheduleUpdate(){return this.performUpdate()}performUpdate(){if(!this.isUpdatePending)return;if(!this.hasUpdated){if(this.renderRoot??=this.createRenderRoot(),this._$Ep){for(const[s,r]of this._$Ep)this[s]=r;this._$Ep=void 0}const n=this.constructor.elementProperties;if(n.size>0)for(const[s,r]of n){const{wrapped:o}=r,a=this[s];o!==!0||this._$AL.has(s)||a===void 0||this.C(s,void 0,r,a)}}let t=!1;const i=this._$AL;try{t=this.shouldUpdate(i),t?(this.willUpdate(i),this._$EO?.forEach(n=>n.hostUpdate?.()),this.update(i)):this._$EM()}catch(n){throw t=!1,this._$EM(),n}t&&this._$AE(i)}willUpdate(t){}_$AE(t){this._$EO?.forEach(i=>i.hostUpdated?.()),this.hasUpdated||(this.hasUpdated=!0,this.firstUpdated(t)),this.updated(t)}_$EM(){this._$AL=new Map,this.isUpdatePending=!1}get updateComplete(){return this.getUpdateComplete()}getUpdateComplete(){return this._$ES}shouldUpdate(t){return!0}update(t){this._$Eq&&=this._$Eq.forEach(i=>this._$ET(i,this[i])),this._$EM()}updated(t){}firstUpdated(t){}};D.elementStyles=[],D.shadowRootOptions={mode:"open"},D[q("elementProperties")]=new Map,D[q("finalized")]=new Map,zt?.({ReactiveElement:D}),(ce.reactiveElementVersions??=[]).push("2.1.2");const Te=globalThis,Le=e=>e,re=Te.trustedTypes,Ue=re?re.createPolicy("lit-html",{createHTML:e=>e}):void 0,ut="$lit$",C=`lit$${Math.random().toFixed(9).slice(2)}$`,ht="?"+C,Jt=`<${ht}>`,O=document,B=()=>O.createComment(""),V=e=>e===null||typeof e!="object"&&typeof e!="function",Ee=Array.isArray,Bt=e=>Ee(e)||typeof e?.[Symbol.iterator]=="function",ge=`[ 	
\f\r]`,L=/<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g,Fe=/-->/g,He=/>/g,I=RegExp(`>|${ge}(?:([^\\s"'>=/]+)(${ge}*=${ge}*(?:[^ 	
\f\r"'\`<>=]|("|')|))|$)`,"g"),qe=/'/g,ze=/"/g,pt=/^(?:script|style|textarea|title)$/i,Vt=e=>(t,...i)=>({_$litType$:e,strings:t,values:i}),h=Vt(1),M=Symbol.for("lit-noChange"),f=Symbol.for("lit-nothing"),Je=new WeakMap,j=O.createTreeWalker(O,129);function ft(e,t){if(!Ee(e)||!e.hasOwnProperty("raw"))throw Error("invalid template strings array");return Ue!==void 0?Ue.createHTML(t):t}const Qt=(e,t)=>{const i=e.length-1,n=[];let s,r=t===2?"<svg>":t===3?"<math>":"",o=L;for(let a=0;a<i;a++){const l=e[a];let c,d,u=-1,p=0;for(;p<l.length&&(o.lastIndex=p,d=o.exec(l),d!==null);)p=o.lastIndex,o===L?d[1]==="!--"?o=Fe:d[1]!==void 0?o=He:d[2]!==void 0?(pt.test(d[2])&&(s=RegExp("</"+d[2],"g")),o=I):d[3]!==void 0&&(o=I):o===I?d[0]===">"?(o=s??L,u=-1):d[1]===void 0?u=-2:(u=o.lastIndex-d[2].length,c=d[1],o=d[3]===void 0?I:d[3]==='"'?ze:qe):o===ze||o===qe?o=I:o===Fe||o===He?o=L:(o=I,s=void 0);const y=o===I&&e[a+1].startsWith("/>")?" ":"";r+=o===L?l+Jt:u>=0?(n.push(c),l.slice(0,u)+ut+l.slice(u)+C+y):l+C+(u===-2?a:y)}return[ft(e,r+(e[i]||"<?>")+(t===2?"</svg>":t===3?"</math>":"")),n]};class Q{constructor({strings:t,_$litType$:i},n){let s;this.parts=[];let r=0,o=0;const a=t.length-1,l=this.parts,[c,d]=Qt(t,i);if(this.el=Q.createElement(c,n),j.currentNode=this.el.content,i===2||i===3){const u=this.el.content.firstChild;u.replaceWith(...u.childNodes)}for(;(s=j.nextNode())!==null&&l.length<a;){if(s.nodeType===1){if(s.hasAttributes())for(const u of s.getAttributeNames())if(u.endsWith(ut)){const p=d[o++],y=s.getAttribute(u).split(C),w=/([.?@])?(.*)/.exec(p);l.push({type:1,index:r,name:w[2],strings:y,ctor:w[1]==="."?Kt:w[1]==="?"?Gt:w[1]==="@"?Yt:de}),s.removeAttribute(u)}else u.startsWith(C)&&(l.push({type:6,index:r}),s.removeAttribute(u));if(pt.test(s.tagName)){const u=s.textContent.split(C),p=u.length-1;if(p>0){s.textContent=re?re.emptyScript:"";for(let y=0;y<p;y++)s.append(u[y],B()),j.nextNode(),l.push({type:2,index:++r});s.append(u[p],B())}}}else if(s.nodeType===8)if(s.data===ht)l.push({type:2,index:r});else{let u=-1;for(;(u=s.data.indexOf(C,u+1))!==-1;)l.push({type:7,index:r}),u+=C.length-1}r++}}static createElement(t,i){const n=O.createElement("template");return n.innerHTML=t,n}}function N(e,t,i=e,n){if(t===M)return t;let s=n!==void 0?i._$Co?.[n]:i._$Cl;const r=V(t)?void 0:t._$litDirective$;return s?.constructor!==r&&(s?._$AO?.(!1),r===void 0?s=void 0:(s=new r(e),s._$AT(e,i,n)),n!==void 0?(i._$Co??=[])[n]=s:i._$Cl=s),s!==void 0&&(t=N(e,s._$AS(e,t.values),s,n)),t}class Wt{constructor(t,i){this._$AV=[],this._$AN=void 0,this._$AD=t,this._$AM=i}get parentNode(){return this._$AM.parentNode}get _$AU(){return this._$AM._$AU}u(t){const{el:{content:i},parts:n}=this._$AD,s=(t?.creationScope??O).importNode(i,!0);j.currentNode=s;let r=j.nextNode(),o=0,a=0,l=n[0];for(;l!==void 0;){if(o===l.index){let c;l.type===2?c=new G(r,r.nextSibling,this,t):l.type===1?c=new l.ctor(r,l.name,l.strings,this,t):l.type===6&&(c=new Zt(r,this,t)),this._$AV.push(c),l=n[++a]}o!==l?.index&&(r=j.nextNode(),o++)}return j.currentNode=O,s}p(t){let i=0;for(const n of this._$AV)n!==void 0&&(n.strings!==void 0?(n._$AI(t,n,i),i+=n.strings.length-2):n._$AI(t[i])),i++}}class G{get _$AU(){return this._$AM?._$AU??this._$Cv}constructor(t,i,n,s){this.type=2,this._$AH=f,this._$AN=void 0,this._$AA=t,this._$AB=i,this._$AM=n,this.options=s,this._$Cv=s?.isConnected??!0}get parentNode(){let t=this._$AA.parentNode;const i=this._$AM;return i!==void 0&&t?.nodeType===11&&(t=i.parentNode),t}get startNode(){return this._$AA}get endNode(){return this._$AB}_$AI(t,i=this){t=N(this,t,i),V(t)?t===f||t==null||t===""?(this._$AH!==f&&this._$AR(),this._$AH=f):t!==this._$AH&&t!==M&&this._(t):t._$litType$!==void 0?this.$(t):t.nodeType!==void 0?this.T(t):Bt(t)?this.k(t):this._(t)}O(t){return this._$AA.parentNode.insertBefore(t,this._$AB)}T(t){this._$AH!==t&&(this._$AR(),this._$AH=this.O(t))}_(t){this._$AH!==f&&V(this._$AH)?this._$AA.nextSibling.data=t:this.T(O.createTextNode(t)),this._$AH=t}$(t){const{values:i,_$litType$:n}=t,s=typeof n=="number"?this._$AC(t):(n.el===void 0&&(n.el=Q.createElement(ft(n.h,n.h[0]),this.options)),n);if(this._$AH?._$AD===s)this._$AH.p(i);else{const r=new Wt(s,this),o=r.u(this.options);r.p(i),this.T(o),this._$AH=r}}_$AC(t){let i=Je.get(t.strings);return i===void 0&&Je.set(t.strings,i=new Q(t)),i}k(t){Ee(this._$AH)||(this._$AH=[],this._$AR());const i=this._$AH;let n,s=0;for(const r of t)s===i.length?i.push(n=new G(this.O(B()),this.O(B()),this,this.options)):n=i[s],n._$AI(r),s++;s<i.length&&(this._$AR(n&&n._$AB.nextSibling,s),i.length=s)}_$AR(t=this._$AA.nextSibling,i){for(this._$AP?.(!1,!0,i);t!==this._$AB;){const n=Le(t).nextSibling;Le(t).remove(),t=n}}setConnected(t){this._$AM===void 0&&(this._$Cv=t,this._$AP?.(t))}}class de{get tagName(){return this.element.tagName}get _$AU(){return this._$AM._$AU}constructor(t,i,n,s,r){this.type=1,this._$AH=f,this._$AN=void 0,this.element=t,this.name=i,this._$AM=s,this.options=r,n.length>2||n[0]!==""||n[1]!==""?(this._$AH=Array(n.length-1).fill(new String),this.strings=n):this._$AH=f}_$AI(t,i=this,n,s){const r=this.strings;let o=!1;if(r===void 0)t=N(this,t,i,0),o=!V(t)||t!==this._$AH&&t!==M,o&&(this._$AH=t);else{const a=t;let l,c;for(t=r[0],l=0;l<r.length-1;l++)c=N(this,a[n+l],i,l),c===M&&(c=this._$AH[l]),o||=!V(c)||c!==this._$AH[l],c===f?t=f:t!==f&&(t+=(c??"")+r[l+1]),this._$AH[l]=c}o&&!s&&this.j(t)}j(t){t===f?this.element.removeAttribute(this.name):this.element.setAttribute(this.name,t??"")}}class Kt extends de{constructor(){super(...arguments),this.type=3}j(t){this.element[this.name]=t===f?void 0:t}}class Gt extends de{constructor(){super(...arguments),this.type=4}j(t){this.element.toggleAttribute(this.name,!!t&&t!==f)}}class Yt extends de{constructor(t,i,n,s,r){super(t,i,n,s,r),this.type=5}_$AI(t,i=this){if((t=N(this,t,i,0)??f)===M)return;const n=this._$AH,s=t===f&&n!==f||t.capture!==n.capture||t.once!==n.once||t.passive!==n.passive,r=t!==f&&(n===f||s);s&&this.element.removeEventListener(this.name,this,n),r&&this.element.addEventListener(this.name,this,t),this._$AH=t}handleEvent(t){typeof this._$AH=="function"?this._$AH.call(this.options?.host??this.element,t):this._$AH.handleEvent(t)}}class Zt{constructor(t,i,n){this.element=t,this.type=6,this._$AN=void 0,this._$AM=i,this.options=n}get _$AU(){return this._$AM._$AU}_$AI(t){N(this,t)}}const Xt=Te.litHtmlPolyfillSupport;Xt?.(Q,G),(Te.litHtmlVersions??=[]).push("3.3.3");const ei=(e,t,i)=>{const n=i?.renderBefore??t;let s=n._$litPart$;if(s===void 0){const r=i?.renderBefore??null;n._$litPart$=s=new G(t.insertBefore(B(),r),r,void 0,i??{})}return s._$AI(e),s};const Pe=globalThis;class z extends D{constructor(){super(...arguments),this.renderOptions={host:this},this._$Do=void 0}createRenderRoot(){const t=super.createRenderRoot();return this.renderOptions.renderBefore??=t.firstChild,t}update(t){const i=this.render();this.hasUpdated||(this.renderOptions.isConnected=this.isConnected),super.update(t),this._$Do=ei(i,this.renderRoot,this.renderOptions)}connectedCallback(){super.connectedCallback(),this._$Do?.setConnected(!0)}disconnectedCallback(){super.disconnectedCallback(),this._$Do?.setConnected(!1)}render(){return M}}z._$litElement$=!0,z.finalized=!0,Pe.litElementHydrateSupport?.({LitElement:z});const ti=Pe.litElementPolyfillSupport;ti?.({LitElement:z});(Pe.litElementVersions??=[]).push("4.2.2");const ii=e=>(t,i)=>{i!==void 0?i.addInitializer(()=>{customElements.define(e,t)}):customElements.define(e,t)};const si={attribute:!0,type:String,converter:ne,reflect:!1,hasChanged:Ce},ni=(e=si,t,i)=>{const{kind:n,metadata:s}=i;let r=globalThis.litPropertyMetadata.get(s);if(r===void 0&&globalThis.litPropertyMetadata.set(s,r=new Map),n==="setter"&&((e=Object.create(e)).wrapped=!0),r.set(i.name,e),n==="accessor"){const{name:o}=i;return{set(a){const l=t.get.call(this);t.set.call(this,a),this.requestUpdate(o,l,e,!0,a)},init(a){return a!==void 0&&this.C(o,void 0,e,a),a}}}if(n==="setter"){const{name:o}=i;return function(a){const l=this[o];t.call(this,a),this.requestUpdate(o,l,e,!0,a)}}throw Error("Unsupported decorator location: "+n)};function ri(e){return(t,i)=>typeof i=="object"?ni(e,t,i):((n,s,r)=>{const o=s.hasOwnProperty(r);return s.constructor.createProperty(r,n),o?Object.getOwnPropertyDescriptor(s,r):void 0})(e,t,i)}function v(e){return ri({...e,state:!0,attribute:!1})}const oi={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3a6 6 0 0 0 9 9a9 9 0 1 1-9-9Z"/>'},ai={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="13.5" cy="6.5" r=".5"/><circle cx="17.5" cy="10.5" r=".5"/><circle cx="8.5" cy="7.5" r=".5"/><circle cx="6.5" cy="12.5" r=".5"/><path d="M12 2C6.5 2 2 6.5 2 12s4.5 10 10 10c.926 0 1.648-.746 1.648-1.688c0-.437-.18-.835-.437-1.125c-.29-.289-.438-.652-.438-1.125a1.64 1.64 0 0 1 1.668-1.668h1.996c3.051 0 5.555-2.503 5.555-5.554C21.965 6.012 17.461 2 12 2z"/></g>'},Be={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 4h4v16H6zm8 0h4v16h-4z"/>'},Ve={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m5 3l14 9l-14 9V3z"/>'},li={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M4.93 4.93l1.41 1.41m11.32 11.32l1.41 1.41M2 12h2m16 0h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/></g>'},ci={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 6h18m-2 0v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6m3 0V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2m-6 5v6m4-6v6"/>'},Qe={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18 6L6 18M6 6l12 12"/>'};const gt=Object.freeze({left:0,top:0,width:16,height:16}),oe=Object.freeze({rotate:0,vFlip:!1,hFlip:!1}),Y=Object.freeze({...gt,...oe}),ye=Object.freeze({...Y,body:"",hidden:!1}),di=Object.freeze({width:null,height:null}),mt=Object.freeze({...di,...oe});function ui(e,t=0){const i=e.replace(/^-?[0-9.]*/,"");function n(s){for(;s<0;)s+=4;return s%4}if(i===""){const s=parseInt(e);return isNaN(s)?0:n(s)}else if(i!==e){let s=0;switch(i){case"%":s=25;break;case"deg":s=90}if(s){let r=parseFloat(e.slice(0,e.length-i.length));return isNaN(r)?0:(r=r/s,r%1===0?n(r):0)}}return t}const hi=/[\s,]+/;function pi(e,t){t.split(hi).forEach(i=>{switch(i.trim()){case"horizontal":e.hFlip=!0;break;case"vertical":e.vFlip=!0;break}})}const bt={...mt,preserveAspectRatio:""};function We(e){const t={...bt},i=(n,s)=>e.getAttribute(n)||s;return t.width=i("width",null),t.height=i("height",null),t.rotate=ui(i("rotate","")),pi(t,i("flip","")),t.preserveAspectRatio=i("preserveAspectRatio",i("preserveaspectratio","")),t}function fi(e,t){for(const i in bt)if(e[i]!==t[i])return!0;return!1}const vt=/^[a-z0-9]+(-[a-z0-9]+)*$/,Z=(e,t,i,n="")=>{const s=e.split(":");if(e.slice(0,1)==="@"){if(s.length<2||s.length>3)return null;n=s.shift().slice(1)}if(s.length>3||!s.length)return null;if(s.length>1){const a=s.pop(),l=s.pop(),c={provider:s.length>0?s[0]:n,prefix:l,name:a};return t&&!te(c)?null:c}const r=s[0],o=r.split("-");if(o.length>1){const a={provider:n,prefix:o.shift(),name:o.join("-")};return t&&!te(a)?null:a}if(i&&n===""){const a={provider:n,prefix:"",name:r};return t&&!te(a,i)?null:a}return null},te=(e,t)=>e?!!((t&&e.prefix===""||e.prefix)&&e.name):!1;function gi(e,t){const i=e.icons,n=e.aliases||Object.create(null),s=Object.create(null);function r(o){if(i[o])return s[o]=[];if(!(o in s)){s[o]=null;const a=n[o]&&n[o].parent,l=a&&r(a);l&&(s[o]=[a].concat(l))}return s[o]}return Object.keys(i).concat(Object.keys(n)).forEach(r),s}function mi(e,t){const i={};!e.hFlip!=!t.hFlip&&(i.hFlip=!0),!e.vFlip!=!t.vFlip&&(i.vFlip=!0);const n=((e.rotate||0)+(t.rotate||0))%4;return n&&(i.rotate=n),i}function Ke(e,t){const i=mi(e,t);for(const n in ye)n in oe?n in e&&!(n in i)&&(i[n]=oe[n]):n in t?i[n]=t[n]:n in e&&(i[n]=e[n]);return i}function bi(e,t,i){const n=e.icons,s=e.aliases||Object.create(null);let r={};function o(a){r=Ke(n[a]||s[a],r)}return o(t),i.forEach(o),Ke(e,r)}function yt(e,t){const i=[];if(typeof e!="object"||typeof e.icons!="object")return i;e.not_found instanceof Array&&e.not_found.forEach(s=>{t(s,null),i.push(s)});const n=gi(e);for(const s in n){const r=n[s];r&&(t(s,bi(e,s,r)),i.push(s))}return i}const vi={provider:"",aliases:{},not_found:{},...gt};function me(e,t){for(const i in t)if(i in e&&typeof e[i]!=typeof t[i])return!1;return!0}function $t(e){if(typeof e!="object"||e===null)return null;const t=e;if(typeof t.prefix!="string"||!e.icons||typeof e.icons!="object"||!me(e,vi))return null;const i=t.icons;for(const s in i){const r=i[s];if(!s||typeof r.body!="string"||!me(r,ye))return null}const n=t.aliases||Object.create(null);for(const s in n){const r=n[s],o=r.parent;if(!s||typeof o!="string"||!i[o]&&!n[o]||!me(r,ye))return null}return t}const ae=Object.create(null);function yi(e,t){return{provider:e,prefix:t,icons:Object.create(null),missing:new Set}}function A(e,t){const i=ae[e]||(ae[e]=Object.create(null));return i[t]||(i[t]=yi(e,t))}function xt(e,t){return $t(t)?yt(t,(i,n)=>{n?e.icons[i]=n:e.missing.add(i)}):[]}function $i(e,t,i){try{if(typeof i.body=="string")return e.icons[t]={...i},!0}catch{}return!1}function xi(e,t){let i=[];return(typeof e=="string"?[e]:Object.keys(ae)).forEach(n=>{(typeof n=="string"&&typeof t=="string"?[t]:Object.keys(ae[n]||{})).forEach(s=>{const r=A(n,s);i=i.concat(Object.keys(r.icons).map(o=>(n!==""?"@"+n+":":"")+s+":"+o))})}),i}let W=!1;function wt(e){return typeof e=="boolean"&&(W=e),W}function K(e){const t=typeof e=="string"?Z(e,!0,W):e;if(t){const i=A(t.provider,t.prefix),n=t.name;return i.icons[n]||(i.missing.has(n)?null:void 0)}}function _t(e,t){const i=Z(e,!0,W);if(!i)return!1;const n=A(i.provider,i.prefix);return t?$i(n,i.name,t):(n.missing.add(i.name),!0)}function Ge(e,t){if(typeof e!="object")return!1;if(typeof t!="string"&&(t=e.provider||""),W&&!t&&!e.prefix){let n=!1;return $t(e)&&(e.prefix="",yt(e,(s,r)=>{_t(s,r)&&(n=!0)})),n}const i=e.prefix;return te({prefix:i,name:"a"})?!!xt(A(t,i),e):!1}function wi(e){return!!K(e)}function _i(e){const t=K(e);return t&&{...Y,...t}}function kt(e,t){e.forEach(i=>{const n=i.loaderCallbacks;n&&(i.loaderCallbacks=n.filter(s=>s.id!==t))})}function ki(e){e.pendingCallbacksFlag||(e.pendingCallbacksFlag=!0,setTimeout(()=>{e.pendingCallbacksFlag=!1;const t=e.loaderCallbacks?e.loaderCallbacks.slice(0):[];if(!t.length)return;let i=!1;const n=e.provider,s=e.prefix;t.forEach(r=>{const o=r.icons,a=o.pending.length;o.pending=o.pending.filter(l=>{if(l.prefix!==s)return!0;const c=l.name;if(e.icons[c])o.loaded.push({provider:n,prefix:s,name:c});else if(e.missing.has(c))o.missing.push({provider:n,prefix:s,name:c});else return i=!0,!0;return!1}),o.pending.length!==a&&(i||kt([e],r.id),r.callback(o.loaded.slice(0),o.missing.slice(0),o.pending.slice(0),r.abort))})}))}let Si=0;function Ai(e,t,i){const n=Si++,s=kt.bind(null,i,n);if(!t.pending.length)return s;const r={id:n,icons:t,callback:e,abort:s};return i.forEach(o=>{(o.loaderCallbacks||(o.loaderCallbacks=[])).push(r)}),s}function Ci(e){const t={loaded:[],missing:[],pending:[]},i=Object.create(null);e.sort((s,r)=>s.provider!==r.provider?s.provider.localeCompare(r.provider):s.prefix!==r.prefix?s.prefix.localeCompare(r.prefix):s.name.localeCompare(r.name));let n={provider:"",prefix:"",name:""};return e.forEach(s=>{if(n.name===s.name&&n.prefix===s.prefix&&n.provider===s.provider)return;n=s;const r=s.provider,o=s.prefix,a=s.name,l=i[r]||(i[r]=Object.create(null)),c=l[o]||(l[o]=A(r,o));let d;a in c.icons?d=t.loaded:o===""||c.missing.has(a)?d=t.missing:d=t.pending;const u={provider:r,prefix:o,name:a};d.push(u)}),t}const $e=Object.create(null);function Ye(e,t){$e[e]=t}function xe(e){return $e[e]||$e[""]}function Ti(e,t=!0,i=!1){const n=[];return e.forEach(s=>{const r=typeof s=="string"?Z(s,t,i):s;r&&n.push(r)}),n}function Ie(e){let t;if(typeof e.resources=="string")t=[e.resources];else if(t=e.resources,!(t instanceof Array)||!t.length)return null;return{resources:t,path:e.path||"/",maxURL:e.maxURL||500,rotate:e.rotate||750,timeout:e.timeout||5e3,random:e.random===!0,index:e.index||0,dataAfterTimeout:e.dataAfterTimeout!==!1}}const ue=Object.create(null),U=["https://api.simplesvg.com","https://api.unisvg.com"],ie=[];for(;U.length>0;)U.length===1||Math.random()>.5?ie.push(U.shift()):ie.push(U.pop());ue[""]=Ie({resources:["https://api.iconify.design"].concat(ie)});function Ze(e,t){const i=Ie(t);return i===null?!1:(ue[e]=i,!0)}function he(e){return ue[e]}function Ei(){return Object.keys(ue)}const Pi={resources:[],index:0,timeout:2e3,rotate:750,random:!1,dataAfterTimeout:!1};function Ii(e,t,i,n){const s=e.resources.length,r=e.random?Math.floor(Math.random()*s):e.index;let o;if(e.random){let m=e.resources.slice(0);for(o=[];m.length>1;){const k=Math.floor(Math.random()*m.length);o.push(m[k]),m=m.slice(0,k).concat(m.slice(k+1))}o=o.concat(m)}else o=e.resources.slice(r).concat(e.resources.slice(0,r));const a=Date.now();let l="pending",c=0,d,u=null,p=[],y=[];typeof n=="function"&&y.push(n);function w(){u&&(clearTimeout(u),u=null)}function T(){l==="pending"&&(l="aborted"),w(),p.forEach(m=>{m.status==="pending"&&(m.status="aborted")}),p=[]}function x(m,k){k&&(y=[]),typeof m=="function"&&y.push(m)}function pe(){return{startTime:a,payload:t,status:l,queriesSent:c,queriesPending:p.length,subscribe:x,abort:T}}function E(){l="failed",y.forEach(m=>{m(void 0,d)})}function S(){p.forEach(m=>{m.status==="pending"&&(m.status="aborted")}),p=[]}function _(m,k,R){const X=k!=="success";switch(p=p.filter(P=>P!==m),l){case"pending":break;case"failed":if(X||!e.dataAfterTimeout)return;break;default:return}if(k==="abort"){d=R,E();return}if(X){d=R,p.length||(o.length?fe():E());return}if(w(),S(),!e.random){const P=e.resources.indexOf(m.resource);P!==-1&&P!==e.index&&(e.index=P)}l="completed",y.forEach(P=>{P(R)})}function fe(){if(l!=="pending")return;w();const m=o.shift();if(m===void 0){if(p.length){u=setTimeout(()=>{w(),l==="pending"&&(S(),E())},e.timeout);return}E();return}const k={status:"pending",resource:m,callback:(R,X)=>{_(k,R,X)}};p.push(k),c++,u=setTimeout(fe,e.rotate),i(m,t,k.callback)}return setTimeout(fe),pe}function St(e){const t={...Pi,...e};let i=[];function n(){i=i.filter(o=>o().status==="pending")}function s(o,a,l){const c=Ii(t,o,a,(d,u)=>{n(),l&&l(d,u)});return i.push(c),c}function r(o){return i.find(a=>o(a))||null}return{query:s,find:r,setIndex:o=>{t.index=o},getIndex:()=>t.index,cleanup:n}}function Xe(){}const be=Object.create(null);function ji(e){if(!be[e]){const t=he(e);if(!t)return;be[e]={config:t,redundancy:St(t)}}return be[e]}function At(e,t,i){let n,s;if(typeof e=="string"){const r=xe(e);if(!r)return i(void 0,424),Xe;s=r.send;const o=ji(e);o&&(n=o.redundancy)}else{const r=Ie(e);if(r){n=St(r);const o=xe(e.resources?e.resources[0]:"");o&&(s=o.send)}}return!n||!s?(i(void 0,424),Xe):n.query(t,s,i)().abort}function et(){}function Oi(e){e.iconsLoaderFlag||(e.iconsLoaderFlag=!0,setTimeout(()=>{e.iconsLoaderFlag=!1,ki(e)}))}function Di(e){const t=[],i=[];return e.forEach(n=>{(n.match(vt)?t:i).push(n)}),{valid:t,invalid:i}}function F(e,t,i){function n(){const s=e.pendingIcons;t.forEach(r=>{s&&s.delete(r),e.icons[r]||e.missing.add(r)})}if(i&&typeof i=="object")try{if(!xt(e,i).length){n();return}}catch(s){console.error(s)}n(),Oi(e)}function tt(e,t){e instanceof Promise?e.then(i=>{t(i)}).catch(()=>{t(null)}):t(e)}function Mi(e,t){e.iconsToLoad?e.iconsToLoad=e.iconsToLoad.concat(t).sort():e.iconsToLoad=t,e.iconsQueueFlag||(e.iconsQueueFlag=!0,setTimeout(()=>{e.iconsQueueFlag=!1;const{provider:i,prefix:n}=e,s=e.iconsToLoad;if(delete e.iconsToLoad,!s||!s.length)return;const r=e.loadIcon;if(e.loadIcons&&(s.length>1||!r)){tt(e.loadIcons(s,n,i),c=>{F(e,s,c)});return}if(r){s.forEach(c=>{tt(r(c,n,i),d=>{F(e,[c],d?{prefix:n,icons:{[c]:d}}:null)})});return}const{valid:o,invalid:a}=Di(s);if(a.length&&F(e,a,null),!o.length)return;const l=n.match(vt)?xe(i):null;if(!l){F(e,o,null);return}l.prepare(i,n,o).forEach(c=>{At(i,c,d=>{F(e,c.icons,d)})})}))}const je=(e,t)=>{const i=Ci(Ti(e,!0,wt()));if(!i.pending.length){let a=!0;return t&&setTimeout(()=>{a&&t(i.loaded,i.missing,i.pending,et)}),()=>{a=!1}}const n=Object.create(null),s=[];let r,o;return i.pending.forEach(a=>{const{provider:l,prefix:c}=a;if(c===o&&l===r)return;r=l,o=c,s.push(A(l,c));const d=n[l]||(n[l]=Object.create(null));d[c]||(d[c]=[])}),i.pending.forEach(a=>{const{provider:l,prefix:c,name:d}=a,u=A(l,c),p=u.pendingIcons||(u.pendingIcons=new Set);p.has(d)||(p.add(d),n[l][c].push(d))}),s.forEach(a=>{const l=n[a.provider][a.prefix];l.length&&Mi(a,l)}),t?Ai(t,i,s):et},Ni=e=>new Promise((t,i)=>{const n=typeof e=="string"?Z(e,!0):e;if(!n){i(e);return}je([n||e],s=>{if(s.length&&n){const r=K(n);if(r){t({...Y,...r});return}}i(e)})});function it(e){try{const t=typeof e=="string"?JSON.parse(e):e;if(typeof t.body=="string")return{...t}}catch{}}function Ri(e,t){if(typeof e=="object")return{data:it(e),value:e};if(typeof e!="string")return{value:e};if(e.includes("{")){const r=it(e);if(r)return{data:r,value:e}}const i=Z(e,!0,!0);if(!i)return{value:e};const n=K(i);if(n!==void 0||!i.prefix)return{value:e,name:i,data:n};const s=je([i],()=>t(e,i,K(i)));return{value:e,name:i,loading:s}}let Ct=!1;try{Ct=navigator.vendor.indexOf("Apple")===0}catch{}function Li(e,t){switch(t){case"svg":case"bg":case"mask":return t}return t!=="style"&&(Ct||e.indexOf("<a")===-1)?"svg":e.indexOf("currentColor")===-1?"bg":"mask"}const Ui=/(-?[0-9.]*[0-9]+[0-9.]*)/g,Fi=/^-?[0-9.]*[0-9]+[0-9.]*$/g;function we(e,t,i){if(t===1)return e;if(i=i||100,typeof e=="number")return Math.ceil(e*t*i)/i;if(typeof e!="string")return e;const n=e.split(Ui);if(n===null||!n.length)return e;const s=[];let r=n.shift(),o=Fi.test(r);for(;;){if(o){const a=parseFloat(r);isNaN(a)?s.push(r):s.push(Math.ceil(a*t*i)/i)}else s.push(r);if(r=n.shift(),r===void 0)return s.join("");o=!o}}function Hi(e,t="defs"){let i="";const n=e.indexOf("<"+t);for(;n>=0;){const s=e.indexOf(">",n),r=e.indexOf("</"+t);if(s===-1||r===-1)break;const o=e.indexOf(">",r);if(o===-1)break;i+=e.slice(s+1,r).trim(),e=e.slice(0,n).trim()+e.slice(o+1)}return{defs:i,content:e}}function qi(e,t){return e?"<defs>"+e+"</defs>"+t:t}function zi(e,t,i){const n=Hi(e);return qi(n.defs,t+n.content+i)}const Ji=e=>e==="unset"||e==="undefined"||e==="none";function Tt(e,t){const i={...Y,...e},n={...mt,...t},s={left:i.left,top:i.top,width:i.width,height:i.height};let r=i.body;[i,n].forEach(T=>{const x=[],pe=T.hFlip,E=T.vFlip;let S=T.rotate;pe?E?S+=2:(x.push("translate("+(s.width+s.left).toString()+" "+(0-s.top).toString()+")"),x.push("scale(-1 1)"),s.top=s.left=0):E&&(x.push("translate("+(0-s.left).toString()+" "+(s.height+s.top).toString()+")"),x.push("scale(1 -1)"),s.top=s.left=0);let _;switch(S<0&&(S-=Math.floor(S/4)*4),S=S%4,S){case 1:_=s.height/2+s.top,x.unshift("rotate(90 "+_.toString()+" "+_.toString()+")");break;case 2:x.unshift("rotate(180 "+(s.width/2+s.left).toString()+" "+(s.height/2+s.top).toString()+")");break;case 3:_=s.width/2+s.left,x.unshift("rotate(-90 "+_.toString()+" "+_.toString()+")");break}S%2===1&&(s.left!==s.top&&(_=s.left,s.left=s.top,s.top=_),s.width!==s.height&&(_=s.width,s.width=s.height,s.height=_)),x.length&&(r=zi(r,'<g transform="'+x.join(" ")+'">',"</g>"))});const o=n.width,a=n.height,l=s.width,c=s.height;let d,u;o===null?(u=a===null?"1em":a==="auto"?c:a,d=we(u,l/c)):(d=o==="auto"?l:o,u=a===null?we(d,c/l):a==="auto"?c:a);const p={},y=(T,x)=>{Ji(x)||(p[T]=x.toString())};y("width",d),y("height",u);const w=[s.left,s.top,l,c];return p.viewBox=w.join(" "),{attributes:p,viewBox:w,body:r}}function Oe(e,t){let i=e.indexOf("xlink:")===-1?"":' xmlns:xlink="http://www.w3.org/1999/xlink"';for(const n in t)i+=" "+n+'="'+t[n]+'"';return'<svg xmlns="http://www.w3.org/2000/svg"'+i+">"+e+"</svg>"}function Bi(e){return e.replace(/"/g,"'").replace(/%/g,"%25").replace(/#/g,"%23").replace(/</g,"%3C").replace(/>/g,"%3E").replace(/\s+/g," ")}function Vi(e){return"data:image/svg+xml,"+Bi(e)}function Et(e){return'url("'+Vi(e)+'")'}const Qi=()=>{let e;try{if(e=fetch,typeof e=="function")return e}catch{}};let le=Qi();function Wi(e){le=e}function Ki(){return le}function Gi(e,t){const i=he(e);if(!i)return 0;let n;if(!i.maxURL)n=0;else{let s=0;i.resources.forEach(o=>{s=Math.max(s,o.length)});const r=t+".json?icons=";n=i.maxURL-s-i.path.length-r.length}return n}function Yi(e){return e===404}const Zi=(e,t,i)=>{const n=[],s=Gi(e,t),r="icons";let o={type:r,provider:e,prefix:t,icons:[]},a=0;return i.forEach((l,c)=>{a+=l.length+1,a>=s&&c>0&&(n.push(o),o={type:r,provider:e,prefix:t,icons:[]},a=l.length),o.icons.push(l)}),n.push(o),n};function Xi(e){if(typeof e=="string"){const t=he(e);if(t)return t.path}return"/"}const es=(e,t,i)=>{if(!le){i("abort",424);return}let n=Xi(t.provider);switch(t.type){case"icons":{const r=t.prefix,o=t.icons.join(","),a=new URLSearchParams({icons:o});n+=r+".json?"+a.toString();break}case"custom":{const r=t.uri;n+=r.slice(0,1)==="/"?r.slice(1):r;break}default:i("abort",400);return}let s=503;le(e+n).then(r=>{const o=r.status;if(o!==200){setTimeout(()=>{i(Yi(o)?"abort":"next",o)});return}return s=501,r.json()}).then(r=>{if(typeof r!="object"||r===null){setTimeout(()=>{r===404?i("abort",r):i("next",s)});return}setTimeout(()=>{i("success",r)})}).catch(()=>{i("next",s)})},ts={prepare:Zi,send:es};function is(e,t,i){A(i||"",t).loadIcons=e}function ss(e,t,i){A(i||"",t).loadIcon=e}const ve="data-style";let Pt="";function ns(e){Pt=e}function st(e,t){let i=Array.from(e.childNodes).find(n=>n.hasAttribute&&n.hasAttribute(ve));i||(i=document.createElement("style"),i.setAttribute(ve,ve),e.appendChild(i)),i.textContent=":host{display:inline-block;vertical-align:"+(t?"-0.125em":"0")+"}span,svg{display:block;margin:auto}"+Pt}function It(){Ye("",ts),wt(!0);let e;try{e=window}catch{}if(e){if(e.IconifyPreload!==void 0){const i=e.IconifyPreload,n="Invalid IconifyPreload syntax.";typeof i=="object"&&i!==null&&(i instanceof Array?i:[i]).forEach(s=>{try{(typeof s!="object"||s===null||s instanceof Array||typeof s.icons!="object"||typeof s.prefix!="string"||!Ge(s))&&console.error(n)}catch{console.error(n)}})}if(e.IconifyProviders!==void 0){const i=e.IconifyProviders;if(typeof i=="object"&&i!==null)for(const n in i){const s="IconifyProviders["+n+"] is invalid.";try{const r=i[n];if(typeof r!="object"||!r||r.resources===void 0)continue;Ze(n,r)||console.error(s)}catch{console.error(s)}}}}return{iconLoaded:wi,getIcon:_i,listIcons:xi,addIcon:_t,addCollection:Ge,calculateSize:we,buildIcon:Tt,iconToHTML:Oe,svgToURL:Et,loadIcons:je,loadIcon:Ni,addAPIProvider:Ze,setCustomIconLoader:ss,setCustomIconsLoader:is,appendCustomStyle:ns,_api:{getAPIConfig:he,setAPIModule:Ye,sendAPIQuery:At,setFetch:Wi,getFetch:Ki,listAPIProviders:Ei}}}const _e={"background-color":"currentColor"},jt={"background-color":"transparent"},nt={image:"var(--svg)",repeat:"no-repeat",size:"100% 100%"},rt={"-webkit-mask":_e,mask:_e,background:jt};for(const e in rt){const t=rt[e];for(const i in nt)t[e+"-"+i]=nt[i]}function ot(e){return e?e+(e.match(/^[-0-9.]+$/)?"px":""):"inherit"}function rs(e,t,i){const n=document.createElement("span");let s=e.body;s.indexOf("<a")!==-1&&(s+="<!-- "+Date.now()+" -->");const r=e.attributes,o=Oe(s,{...r,width:t.width+"",height:t.height+""}),a=Et(o),l=n.style,c={"--svg":a,width:ot(r.width),height:ot(r.height),...i?_e:jt};for(const d in c)l.setProperty(d,c[d]);return n}let J;function os(){try{J=window.trustedTypes.createPolicy("iconify",{createHTML:e=>e})}catch{J=null}}function as(e){return J===void 0&&os(),J?J.createHTML(e):e}function ls(e){const t=document.createElement("span"),i=e.attributes;let n="";i.width||(n="width: inherit;"),i.height||(n+="height: inherit;"),n&&(i.style=n);const s=Oe(e.body,i);return t.innerHTML=as(s),t.firstChild}function ke(e){return Array.from(e.childNodes).find(t=>{const i=t.tagName&&t.tagName.toUpperCase();return i==="SPAN"||i==="SVG"})}function at(e,t){const i=t.icon.data,n=t.customisations,s=Tt(i,n);n.preserveAspectRatio&&(s.attributes.preserveAspectRatio=n.preserveAspectRatio);const r=t.renderedMode;let o;r==="svg"?o=ls(s):o=rs(s,{...Y,...i},r==="mask");const a=ke(e);a?o.tagName==="SPAN"&&a.tagName===o.tagName?a.setAttribute("style",o.getAttribute("style")):e.replaceChild(o,a):e.appendChild(o)}function lt(e,t,i){const n=i&&(i.rendered?i:i.lastRender);return{rendered:!1,inline:t,icon:e,lastRender:n}}function cs(e="iconify-icon"){let t,i;try{t=window.customElements,i=window.HTMLElement}catch{return}if(!t||!i)return;const n=t.get(e);if(n)return n;const s=["icon","mode","inline","noobserver","width","height","rotate","flip"],r=class extends i{_shadowRoot;_initialised=!1;_state;_checkQueued=!1;_connected=!1;_observer=null;_visible=!0;constructor(){super();const a=this._shadowRoot=this.attachShadow({mode:"open"}),l=this.hasAttribute("inline");st(a,l),this._state=lt({value:""},l),this._queueCheck()}connectedCallback(){this._connected=!0,this.startObserver()}disconnectedCallback(){this._connected=!1,this.stopObserver()}static get observedAttributes(){return s.slice(0)}attributeChangedCallback(a){switch(a){case"inline":{const l=this.hasAttribute("inline"),c=this._state;l!==c.inline&&(c.inline=l,st(this._shadowRoot,l));break}case"noobserver":{this.hasAttribute("noobserver")?this.startObserver():this.stopObserver();break}default:this._queueCheck()}}get icon(){const a=this.getAttribute("icon");if(a&&a.slice(0,1)==="{")try{return JSON.parse(a)}catch{}return a}set icon(a){typeof a=="object"&&(a=JSON.stringify(a)),this.setAttribute("icon",a)}get inline(){return this.hasAttribute("inline")}set inline(a){a?this.setAttribute("inline","true"):this.removeAttribute("inline")}get observer(){return this.hasAttribute("observer")}set observer(a){a?this.setAttribute("observer","true"):this.removeAttribute("observer")}restartAnimation(){const a=this._state;if(a.rendered){const l=this._shadowRoot;if(a.renderedMode==="svg")try{l.lastChild.setCurrentTime(0);return}catch{}at(l,a)}}get status(){const a=this._state;return a.rendered?"rendered":a.icon.data===null?"failed":"loading"}_queueCheck(){this._checkQueued||(this._checkQueued=!0,setTimeout(()=>{this._check()}))}_check(){if(!this._checkQueued)return;this._checkQueued=!1;const a=this._state,l=this.getAttribute("icon");if(l!==a.icon.value){this._iconChanged(l);return}if(!a.rendered||!this._visible)return;const c=this.getAttribute("mode"),d=We(this);(a.attrMode!==c||fi(a.customisations,d)||!ke(this._shadowRoot))&&this._renderIcon(a.icon,d,c)}_iconChanged(a){const l=Ri(a,(c,d,u)=>{const p=this._state;if(p.rendered||this.getAttribute("icon")!==c)return;const y={value:c,name:d,data:u};y.data?this._gotIconData(y):p.icon=y});l.data?this._gotIconData(l):this._state=lt(l,this._state.inline,this._state)}_forceRender(){if(!this._visible){const a=ke(this._shadowRoot);a&&this._shadowRoot.removeChild(a);return}this._queueCheck()}_gotIconData(a){this._checkQueued=!1,this._renderIcon(a,We(this),this.getAttribute("mode"))}_renderIcon(a,l,c){const d=Li(a.data.body,c),u=this._state.inline;at(this._shadowRoot,this._state={rendered:!0,icon:a,inline:u,customisations:l,attrMode:c,renderedMode:d})}startObserver(){if(!this._observer&&!this.hasAttribute("noobserver"))try{this._observer=new IntersectionObserver(a=>{const l=a.some(c=>c.isIntersecting);l!==this._visible&&(this._visible=l,this._forceRender())}),this._observer.observe(this)}catch{if(this._observer){try{this._observer.disconnect()}catch{}this._observer=null}}}stopObserver(){this._observer&&(this._observer.disconnect(),this._observer=null,this._visible=!0,this._connected&&this._forceRender())}};s.forEach(a=>{a in r.prototype||Object.defineProperty(r.prototype,a,{get:function(){return this.getAttribute(a)},set:function(l){l!==null?this.setAttribute(a,l):this.removeAttribute(a)}})});const o=It();for(const a in o)r[a]=r.prototype[a]=o[a];return t.define(e,r),r}const ds=cs()||It(),{iconLoaded:bs,getIcon:vs,listIcons:ys,addIcon:$s,addCollection:xs,calculateSize:ws,buildIcon:_s,iconToHTML:ks,svgToURL:Ss,loadIcons:As,loadIcon:Cs,setCustomIconLoader:Ts,setCustomIconsLoader:Es,addAPIProvider:Ps,_api:Is}=ds;async function $(e,t){const i=await fetch(e,{...t,headers:{...t?.body?{"content-type":"application/json"}:{},...t?.headers}});if(!i.ok){const n=await i.json().catch(()=>({error:i.statusText}));throw new Error(n.error||i.statusText)}return i.status===204?void 0:i.json()}var us=Object.defineProperty,hs=Object.getOwnPropertyDescriptor,b=(e,t,i,n)=>{for(var s=n>1?void 0:n?hs(t,i):t,r=e.length-1,o;r>=0;r--)(o=e[r])&&(s=(n?o(t,i,s):o(s))||s);return n&&s&&us(t,i,s),s};const se=["system","dark","bright"],ps={system:ai,dark:oi,bright:li},H={overview:"/",alerts:"/alerts",cluster:"/cluster"};function ct(){return Object.entries(H).find(([,e])=>e===window.location.pathname)?.[0]??"overview"}function fs(){const e=localStorage.getItem("upgrid-theme");return se.includes(e)?e:"system"}let g=class extends z{constructor(){super(...arguments),this.targets=[],this.channels=[],this.alerts=[],this.secrets=[],this.joinTokens=[],this.error="",this.live=!1,this.saving=!1,this.channelKind="webhook",this.joinCommand="",this.search="",this.statusFilter="all",this.sort="name",this.selectedIds=new Set,this.activeSection=ct(),this.copied=!1,this.setupMode=!1,this.joining=!1,this.unlimitedUses=!0,this.theme=fs(),this.detailDirty=!1,this.detailInitialState="",this.systemTheme=matchMedia("(prefers-color-scheme: light)"),this.systemThemeChanged=()=>{this.theme==="system"&&this.applyTheme()},this.routeChanged=()=>{this.activeSection=ct()}}connectedCallback(){super.connectedCallback(),this.applyTheme(),this.systemTheme.addEventListener("change",this.systemThemeChanged),window.addEventListener("popstate",this.routeChanged),this.start()}disconnectedCallback(){this.systemTheme.removeEventListener("change",this.systemThemeChanged),window.removeEventListener("popstate",this.routeChanged),this.events?.close(),super.disconnectedCallback()}async start(){try{const e=await $("/api/v1/setup");if(this.setupMode=e.setup,this.setupMode){this.activeSection="cluster",window.history.replaceState(null,"",H.cluster),this.live=!0;return}await this.refresh(),this.connectEvents()}catch(e){this.error=e instanceof Error?e.message:String(e)}}connectEvents(){this.events?.close(),this.events=new EventSource("/api/v1/events"),this.events.addEventListener("state",()=>{this.refresh()}),this.events.onopen=()=>this.live=!0,this.events.onerror=()=>this.live=!1}applyTheme(){const e=this.theme==="system"?this.systemTheme.matches?"bright":"dark":this.theme;this.dataset.theme=e,document.querySelector('meta[name="theme-color"]')?.setAttribute("content",e==="bright"?"#f4f8f6":"#0b1110")}cycleTheme(){this.theme=se[(se.indexOf(this.theme)+1)%se.length],localStorage.setItem("upgrid-theme",this.theme),this.applyTheme()}async refresh(){try{[this.targets,this.channels,this.alerts,this.secrets,this.cluster,this.joinTokens]=await Promise.all([$("/api/v1/targets"),$("/api/v1/channels"),$("/api/v1/alerts"),$("/api/v1/secrets"),$("/api/v1/cluster"),$("/api/v1/join-tokens")]),this.error=""}catch(e){this.error=e instanceof Error?e.message:String(e)}}openTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.showModal()}closeTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.close()}openTarget(e){this.detailDirty=!1,this.selected=e,this.updateComplete.then(()=>{const t=this.renderRoot.querySelector("#detail-dialog"),i=t?.querySelector("form");i&&(this.detailInitialState=this.detailFormState(i)),t?.showModal()})}closeDetailDialog(){this.renderRoot.querySelector("#detail-dialog")?.close(),this.detailDirty=!1,this.detailInitialState="",this.selected=void 0}showDialog(e){this.renderRoot.querySelector(`#${e}`)?.showModal()}dismissOnBackdrop(e){const t=e.currentTarget;e.target===t&&(t.close(),t.id==="detail-dialog"&&(this.detailDirty=!1,this.detailInitialState="",this.selected=void 0))}navigate(e,t){e.preventDefault(),this.activeSection=t,window.history.pushState(null,"",H[t]),this.updateComplete.then(()=>this.renderRoot.querySelector(`#${t}`)?.scrollIntoView({behavior:"smooth",block:"start"}))}closeDialog(e){this.renderRoot.querySelector(`#${e}`)?.close()}toggleMaxRedirects(e){const t=e.currentTarget,i=t.form?.elements.namedItem("max_redirects");i&&(i.disabled=!t.checked),t.form&&this.compareDetailForm(t.form)}detailFormState(e){return JSON.stringify([...new FormData(e).entries()])}compareDetailForm(e){this.detailDirty=this.detailFormState(e)!==this.detailInitialState}updateDetailDirty(e){this.compareDetailForm(e.currentTarget)}async createTarget(e){e.preventDefault();const t=e.currentTarget,i=new FormData(t),n={name:String(i.get("name")),url:String(i.get("url")),method:String(i.get("method")),accepted_statuses:[{start:200,end:299}],follow_redirects:!0,max_redirects:5,interval_seconds:Number(i.get("interval")),timeout_seconds:Number(i.get("timeout")),failure_threshold:Number(i.get("failures")),headers:{},body:null,body_contains:null,skip_tls_verification:!1,notification_channel_ids:[]};this.saving=!0;try{await $("/api/v1/targets",{method:"POST",body:JSON.stringify(n)}),t.reset(),this.closeTargetDialog(),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}async updateTarget(e){if(e.preventDefault(),!this.selected)return;const t=e.currentTarget,i=new FormData(t),n=String(i.get("statuses")).split(",").map(o=>{const[a,l]=o.trim().split("-").map(Number);return{start:a,end:l||a}}),s=i.get("follow_redirects")==="on",r={name:String(i.get("name")),url:String(i.get("url")),method:String(i.get("method")),accepted_statuses:n,follow_redirects:s,max_redirects:s?Number(i.get("max_redirects")):0,interval_seconds:Number(i.get("interval")),timeout_seconds:Number(i.get("timeout")),failure_threshold:Number(i.get("failures")),headers:Object.fromEntries(Object.entries(this.selected.headers).map(([o,a])=>[o,a.kind==="literal"?a.value:{secret_id:a.secret_id}])),body:this.selected.body?.kind==="literal"?this.selected.body.value:this.selected.body?{secret_id:this.selected.body.secret_id}:null,body_contains:String(i.get("body_contains"))||null,skip_tls_verification:i.get("skip_tls_verification")==="on",notification_channel_ids:this.selected.notification_channel_ids};this.saving=!0;try{await $(`/api/v1/targets/${this.selected.id}`,{method:"PUT",body:JSON.stringify(r)}),this.closeDetailDialog(),await this.refresh()}catch(o){this.error=o instanceof Error?o.message:String(o)}finally{this.saving=!1}}async deleteTarget(){if(!(!this.selected||!window.confirm("Delete this target and its history?"))){this.saving=!0;try{await $(`/api/v1/targets/${this.selected.id}`,{method:"DELETE"}),this.closeDetailDialog(),await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async setPaused(e){if(this.selected){this.saving=!0;try{await $(`/api/v1/targets/${this.selected.id}/${e?"pause":"resume"}`,{method:"POST"}),this.closeDetailDialog(),await this.refresh()}catch(t){this.error=t instanceof Error?t.message:String(t)}finally{this.saving=!1}}}async createSecret(e){e.preventDefault();const t=e.currentTarget,i=new FormData(t);this.saving=!0;try{await $("/api/v1/secrets",{method:"POST",body:JSON.stringify({name:i.get("name"),value:i.get("value")})}),t.reset(),this.closeDialog("secret-dialog"),await this.refresh()}catch(n){this.error=n instanceof Error?n.message:String(n)}finally{this.saving=!1}}async createChannel(e){e.preventDefault();const t=e.currentTarget,i=new FormData(t),n=this.channelKind==="telegram"?{type:"telegram",name:i.get("name"),bot_token:i.get("bot_token"),chat_id:i.get("chat_id")}:{type:"webhook",name:i.get("name"),url:i.get("url"),headers:{}};this.saving=!0;try{await $("/api/v1/channels",{method:"POST",body:JSON.stringify(n)}),t.reset(),this.channelKind="webhook",this.closeDialog("channel-dialog"),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}openTokenDialog(){this.unlimitedUses=!0,this.showDialog("token-config-dialog")}async createJoinToken(e){e.preventDefault();const t=e.currentTarget,i=new FormData(t),n=Number(i.get("expiration"))*Number(i.get("unit")),s=this.unlimitedUses?null:Number(i.get("max_uses"));this.saving=!0;try{const r=await $("/api/v1/join-tokens",{method:"POST",body:JSON.stringify({expires_in_seconds:n,max_uses:s})});this.joinCommand=`upgrid --join '${r.url}'`,this.copied=!1,await this.refresh(),this.closeDialog("token-config-dialog"),this.showDialog("join-dialog")}catch(r){this.error=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}async joinCluster(e){e.preventDefault();const t=e.currentTarget,i=String(new FormData(t).get("join_link")).trim();this.joining=!0;try{await $("/api/v1/cluster/join",{method:"POST",body:JSON.stringify({join_link:i})}),this.closeDialog("join-cluster-dialog");for(let n=0;n<120;n+=1){await new Promise(s=>window.setTimeout(s,250));try{await $("/api/v1/cluster"),window.location.replace(H.cluster);return}catch{}}throw new Error("Cluster join did not finish within 30 seconds")}catch(n){this.error=n instanceof Error?n.message:String(n),this.joining=!1}}async revokeJoinToken(e){if(window.confirm("Revoke this Join Token? Nodes using it will no longer be admitted.")){this.saving=!0;try{await $(`/api/v1/join-tokens/${e.id}`,{method:"DELETE"}),await this.refresh()}catch(t){this.error=t instanceof Error?t.message:String(t)}finally{this.saving=!1}}}async copyJoinCommand(){let e=!1;try{await navigator.clipboard.writeText(this.joinCommand),e=!0}catch{const t=document.createElement("textarea");t.value=this.joinCommand,t.style.position="fixed",t.style.opacity="0",document.body.append(t),t.select(),e=document.execCommand("copy"),t.remove()}if(!e){this.error="Could not copy the Join command";return}this.copied=!0,window.setTimeout(()=>this.copied=!1,2e3)}toggleSelected(e,t){const i=new Set(this.selectedIds);t?i.add(e):i.delete(e),this.selectedIds=i}async bulkPause(e){this.saving=!0;try{await Promise.all([...this.selectedIds].map(t=>$(`/api/v1/targets/${t}/${e?"pause":"resume"}`,{method:"POST"}))),this.selectedIds=new Set,await this.refresh()}catch(t){this.error=t instanceof Error?t.message:String(t)}finally{this.saving=!1}}async bulkDelete(){if(window.confirm(`Delete ${this.selectedIds.size} selected Targets and their history?`)){this.saving=!0;try{await Promise.all([...this.selectedIds].map(e=>$(`/api/v1/targets/${e}`,{method:"DELETE"}))),this.selectedIds=new Set,await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async deleteResource(e,t,i){if(window.confirm(`Delete ${i}?`))try{await $(`/api/v1/${e}/${t}`,{method:"DELETE"}),await this.refresh()}catch(n){this.error=n instanceof Error?n.message:String(n)}}render(){const e=this.targets.filter(r=>r.availability==="up").length,t=this.targets.filter(r=>r.availability==="down").length,i=this.alerts.filter(r=>r.delivery==="pending").length,n=this.setupMode?["cluster"]:["overview","alerts","cluster"],s=this.targets.filter(r=>`${r.name} ${r.url}`.toLowerCase().includes(this.search.toLowerCase())).filter(r=>this.statusFilter==="all"?!0:this.statusFilter==="paused"?r.paused:r.availability===this.statusFilter).sort((r,o)=>this.sort==="status"&&r.availability.localeCompare(o.availability)||r.name.localeCompare(o.name));return h`
      <main class="shell">
        <header>
          <div class="brand">
            <img src="/favicon.svg" alt="" />
            <div>
              <div class="brand-line"><strong>UpGrid</strong><div class="live"><i class="dot ${this.live?"on":""}"></i>${this.live?"live":"connecting"}</div></div>
              <span>Distributed service monitoring</span>
            </div>
          </div>
          <nav aria-label="Primary">
            ${n.map(r=>h`<a class=${this.activeSection===r?"active":""} href=${H[r]} @click=${o=>this.navigate(o,r)}>${r[0].toUpperCase()}${r.slice(1)}</a>`)}
          </nav>
          <div class="actions">
            <button class="button secondary icon-button" aria-label=${`Theme: ${this.theme[0].toUpperCase()}${this.theme.slice(1)}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${ps[this.theme]} aria-hidden="true"></iconify-icon></button>
          </div>
        </header>
        ${this.error?h`<div class="notice" role="alert">${this.error}</div>`:f}
        ${this.activeSection==="overview"?this.renderOverview(s,e,t,i):this.activeSection==="alerts"?this.renderAlertsPage():this.renderClusterPage()}
      </main>
      <dialog id="target-dialog" aria-labelledby="add-target-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="add-target-title">Add target</h2><p>Start monitoring an HTTP or HTTPS endpoint.</p></div>
        <form @submit=${this.createTarget}>
          <label>Name<input name="name" placeholder="Production API" required /></label>
          <label>URL<input name="url" type="url" placeholder="https://example.com/health" required /></label>
          <div class="row">
            <label>Method<input name="method" value="GET" required /></label>
            <label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label>
          </div>
          <div class="row">
            <label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label>
            <label>Failures before Down<input name="failures" type="number" min="1" value="3" required /></label>
          </div>
          <div class="dialog-actions">
            <button class="button secondary" type="button" @click=${this.closeTargetDialog}>Cancel</button>
            <button class="button" type="submit" ?disabled=${this.saving}>${this.saving?"Creating…":"Create target"}</button>
          </div>
        </form>
      </dialog>
      ${this.selected?this.renderDetail(this.selected):f}
      <dialog id="secret-dialog" aria-labelledby="secret-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="secret-title">Add secret</h2><p>The plaintext is encrypted before replication and never returned.</p></div>
        <form @submit=${this.createSecret}>
          <label>Name<input name="name" placeholder="Webhook token" required /></label>
          <label>Value<input name="value" type="password" autocomplete="new-password" required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("secret-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>Create secret</button></div>
        </form>
      </dialog>
      <dialog id="channel-dialog" aria-labelledby="channel-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="channel-title">Add channel</h2><p>Send transitions through Telegram or a generic webhook.</p></div>
        <form @submit=${this.createChannel}>
          <label>Type<select name="type" @change=${r=>this.channelKind=r.target.value}><option value="webhook">Webhook</option><option value="telegram">Telegram</option></select></label>
          <label>Name<input name="name" placeholder="On-call" required /></label>
          ${this.channelKind==="webhook"?h`<label>Webhook URL<input name="url" type="url" placeholder="https://hooks.example.com/upgrid" required /></label>`:h`<label>Bot token<input name="bot_token" type="password" autocomplete="off" required /></label><label>Chat ID<input name="chat_id" required /></label>`}
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("channel-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>Create channel</button></div>
        </form>
      </dialog>
      <dialog id="token-config-dialog" aria-labelledby="token-config-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="token-config-title">Create Join Token</h2><p>Choose how long the token remains valid and how many Nodes it may admit.</p></div>
        <form @submit=${this.createJoinToken}>
          <div class="row">
            <label>Expiration<input name="expiration" type="number" min="1" step="1" value="1" required /></label>
            <label>Unit<select name="unit"><option value="1">Seconds</option><option value="60">Minutes</option><option value="3600">Hours</option><option value="86400" selected>Days</option></select></label>
          </div>
          <label>Usage<select name="usage" @change=${r=>this.unlimitedUses=r.target.value==="unlimited"}><option value="unlimited">Unlimited</option><option value="limited">Limited</option></select></label>
          <label>Maximum uses<input name="max_uses" type="number" min="1" step="1" value="1" ?disabled=${this.unlimitedUses} required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("token-config-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>${this.saving?"Creating…":"Create token"}</button></div>
        </form>
      </dialog>
      <dialog id="join-dialog" aria-labelledby="join-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="join-title">Join Token Created</h2><p>This command contains Cluster credentials. Revoke the token when no longer needed.</p></div>
        <div class="join-command">${this.joinCommand}</div>
        <div class="dialog-actions" style="padding: 0 22px 22px"><button class="button secondary" @click=${()=>this.closeDialog("join-dialog")}>Close</button><button class="button" @click=${this.copyJoinCommand}>${this.copied?"Copied":"Copy command"}</button></div>
      </dialog>
      <dialog id="join-cluster-dialog" aria-labelledby="join-cluster-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="join-cluster-title">Join Cluster</h2><p>Paste an <code>up://</code> Join Token issued by the destination Cluster.</p></div>
        <form @submit=${this.joinCluster}>
          <label>Join Token<input name="join_link" type="url" pattern="up://.*" placeholder="up://node.example/token" autocomplete="off" required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("join-cluster-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.joining}>${this.joining?"Joining…":"Join cluster"}</button></div>
        </form>
      </dialog>
    `}renderOverview(e,t,i,n){const s=this.targets.filter(a=>this.selectedIds.has(a.id)),r=s.some(a=>!a.paused),o=s.some(a=>a.paused);return h`
      <section class="heading" id="overview">
        <div><span class="eyebrow">Cluster status</span><h1>Overview</h1></div>
        <button class="button" @click=${this.openTargetDialog}>Add target</button>
      </section>
      <section class="summary" aria-label="Target summary">
        <div class="metric"><span>Targets</span><strong>${this.targets.length}</strong></div>
        <div class="metric"><span>Up</span><strong>${t}</strong></div>
        <div class="metric"><span>Down</span><strong>${i}</strong></div>
        <div class="metric"><span>Pending alerts</span><strong>${n}</strong></div>
      </section>
      <section class="panel" aria-label="Targets">
        <div class="panel-head"><h2>Targets</h2><span class="meta">${this.targets.length} configured</span></div>
        <div class="toolbar">
          <input aria-label="Search targets" type="search" placeholder="Search name or URL" .value=${this.search} @input=${a=>this.search=a.target.value} />
          <select aria-label="Filter targets" .value=${this.statusFilter} @change=${a=>this.statusFilter=a.target.value}><option value="all">All states</option><option value="up">Up</option><option value="down">Down</option><option value="unknown">Unknown</option><option value="paused">Paused</option></select>
          <select aria-label="Sort targets" .value=${this.sort} @change=${a=>this.sort=a.target.value}><option value="name">Sort by name</option><option value="status">Sort by status</option></select>
        </div>
        ${this.selectedIds.size?h`<div class="bulk"><span class="meta">${this.selectedIds.size} selected</span><div class="bulk-actions"><button class="button secondary icon-button" aria-label="Unselect all" title="Unselect all" @click=${()=>this.selectedIds=new Set}><iconify-icon .icon=${Qe} aria-hidden="true"></iconify-icon></button>${r?h`<button class="button warning icon-button" aria-label="Pause selected" title="Pause selected" @click=${()=>this.bulkPause(!0)}><iconify-icon .icon=${Be} aria-hidden="true"></iconify-icon></button>`:f}${o?h`<button class="button success icon-button" aria-label="Resume selected" title="Resume selected" @click=${()=>this.bulkPause(!1)}><iconify-icon .icon=${Ve} aria-hidden="true"></iconify-icon></button>`:f}<button class="button danger" @click=${this.bulkDelete}>Delete selected</button></div></div>`:f}
        ${e.length?e.map(a=>this.renderTarget(a)):h`<div class="empty">${this.targets.length?"No Targets match these filters.":"No targets yet. Add the first one to begin monitoring."}</div>`}
      </section>
      <section class="resources" aria-label="Notification configuration">
        <section class="panel">
          <div class="panel-head"><h2>Notification channels</h2><button class="button secondary" @click=${()=>this.showDialog("channel-dialog")}>Add channel</button></div>
          ${this.channels.length?this.channels.map(a=>h`<div class="resource"><div><strong>${a.name}</strong><code>${a.destination}</code></div><div class="actions"><span class="badge">${a.kind}</span><button class="button danger" aria-label=${`Delete channel ${a.name}`} @click=${()=>this.deleteResource("channels",a.id,a.name)}>Delete</button></div></div>`):h`<div class="empty">No notification channels.</div>`}
        </section>
        <section class="panel">
          <div class="panel-head"><h2>Secrets</h2><button class="button secondary" @click=${()=>this.showDialog("secret-dialog")}>Add secret</button></div>
          ${this.secrets.length?this.secrets.map(a=>h`<div class="resource"><div><strong>${a.name}</strong><code>${a.id}</code></div><div class="actions"><span class="badge">write-only</span><button class="button danger" aria-label=${`Delete secret ${a.name}`} @click=${()=>this.deleteResource("secrets",a.id,a.name)}>Delete</button></div></div>`):h`<div class="empty">No reusable Secrets.</div>`}
        </section>
      </section>
    `}renderAlertsPage(){return h`
      <section class="heading" id="alerts">
        <div><span class="eyebrow">Delivery history</span><h1>Alerts</h1></div>
      </section>
      <section class="panel" aria-label="Alert history">
        <div class="panel-head"><h2>Availability transitions</h2><span class="meta">${this.alerts.length} events</span></div>
        ${this.alerts.length?this.alerts.map(e=>h`<div class="resource"><div><strong>${e.target_name}</strong><code>${new Date(e.scheduled_at_ms).toLocaleString()}</code></div><span class="badge">${e.kind} · ${e.delivery}</span></div>`):h`<div class="empty">No availability transitions.</div>`}
      </section>
    `}renderClusterPage(){return h`
      <section class="heading" id="cluster">
        <div><span class="eyebrow">Raft membership</span><h1>Cluster</h1></div>
        <div class="actions">
          ${this.setupMode?f:h`<button class="button secondary" @click=${this.openTokenDialog}>Create token</button>`}
          <button class="button" @click=${()=>this.showDialog("join-cluster-dialog")}>Join cluster</button>
        </div>
      </section>
      <section class="panel" aria-label="Cluster topology">
        <div class="panel-head"><h2>Nodes</h2><span class="meta">${this.cluster?.members.length??0} members</span></div>
        ${this.cluster?.members.map(e=>h`<div class="resource"><div><strong>${e.name}</strong><code>${e.raft_url}</code></div><div class="actions">${e.local?h`<span class="badge">This node</span>`:f}${e.leader?h`<span class="badge">Leader</span>`:f}</div></div>`)}
        ${this.cluster?.members.length?f:h`<div class="empty">${this.setupMode?this.joining?"Joining the Cluster…":"This fresh Node is ready to join a Cluster.":"Cluster topology unavailable."}</div>`}
      </section>
      ${this.setupMode?f:h`<section class="panel" aria-label="Join tokens" style="margin-top: 18px">
        <div class="panel-head"><h2>Join Tokens</h2><span class="meta">${this.joinTokens.length} stored</span></div>
        ${this.joinTokens.length?this.joinTokens.map(e=>h`
              <div class="resource">
                <div><strong>${e.id.slice(0,12)}…</strong><code>Expires ${new Date(e.expires_at_ms).toLocaleString()} · ${e.remaining_uses===null?"unlimited uses":`${e.remaining_uses} uses left`}</code></div>
                <button class="button danger" aria-label=${`Revoke Join Token ${e.id.slice(0,12)}`} @click=${()=>this.revokeJoinToken(e)}>Revoke</button>
              </div>
            `):h`<div class="empty">No Join Tokens.</div>`}
      </section>`}
    `}renderTarget(e){const t=e.latest_evaluation,i=e.history.slice(0,16).reverse(),n=Math.max(1,...i.map(s=>s.latency_ms));return h`
      <div class="target-wrap">
        <input class="select-target" type="checkbox" aria-label=${`Select ${e.name}`} .checked=${this.selectedIds.has(e.id)} @change=${s=>this.toggleSelected(e.id,s.target.checked)} />
        <button class="target" aria-label=${e.name} @click=${()=>this.openTarget(e)}>
          <i class="state ${e.paused?"paused":e.availability}" aria-label=${e.paused?"paused":e.availability}></i>
          <div>
            <h3>${e.name}</h3>
            <div class="meta">${e.paused?"Paused · ":""}${e.method} · ${e.url} · every ${e.interval_seconds}s</div>
          </div>
          <div class="target-side">
            ${i.length?h`<div class="mini-chart" aria-hidden="true">${i.map(s=>h`<i class="mini-bar ${s.succeeded?"up":"down"}" style=${`height: ${Math.max(12,s.latency_ms/n*100)}%`}></i>`)}</div>`:f}
            <div class="latency">
              <strong>${t?`${t.latency_ms} ms`:"—"}</strong>
              <span>${t?t.status_code??"network error":"waiting"}</span>
            </div>
          </div>
        </button>
      </div>
    `}renderDetail(e){const t=e.accepted_statuses.map(o=>o.start===o.end?o.start:`${o.start}-${o.end}`).join(","),i=e.history.slice(0,30).reverse(),n=Math.max(1,...i.map(o=>o.latency_ms)),s=o=>new Date(o).toLocaleString(void 0,{month:"short",day:"numeric",hour:"2-digit",minute:"2-digit"}),r=o=>o>=1e3?`${(o/1e3).toFixed(o>=1e4?0:1)} s`:`${Math.round(o)} ms`;return h`
      <dialog id="detail-dialog" aria-labelledby="target-detail-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head">
          <h2 id="target-detail-title">Target details</h2>
          <button class="button secondary icon-button dialog-close" type="button" aria-label="Close target details" title="Close" @click=${this.closeDetailDialog}><iconify-icon .icon=${Qe} aria-hidden="true"></iconify-icon></button>
        </div>
        <form @submit=${this.updateTarget} @input=${this.updateDetailDirty}>
          <label>Name<input name="name" .value=${e.name} required /></label>
          <label>URL<input name="url" type="url" .value=${e.url} required /></label>
          <div class="row">
            <label>Method<input name="method" .value=${e.method} required /></label>
            <label>Expected statuses<input name="statuses" .value=${t} required /></label>
          </div>
          <div class="row">
            <label>Interval (seconds)<input name="interval" type="number" min="1" .value=${String(e.interval_seconds)} required /></label>
            <label>Timeout (seconds)<input name="timeout" type="number" min="1" .value=${String(e.timeout_seconds)} required /></label>
          </div>
          <div class="row">
            <label>Failures before Down<input name="failures" type="number" min="1" .value=${String(e.failure_threshold)} required /></label>
            <label>Maximum redirects<input name="max_redirects" type="number" min="0" .value=${String(e.max_redirects)} ?disabled=${!e.follow_redirects} required /></label>
          </div>
          <label>Body must contain<input name="body_contains" .value=${e.body_contains??""} /></label>
          <div class="row">
            <label class="check"><input name="follow_redirects" type="checkbox" .checked=${e.follow_redirects} @change=${this.toggleMaxRedirects} />Follow redirects</label>
            <label class="check"><input name="skip_tls_verification" type="checkbox" .checked=${e.skip_tls_verification} />Skip TLS verification</label>
          </div>
          <div class="dialog-actions">
            <div class="danger-actions">
              <button class="button danger icon-button" type="button" aria-label="Delete target" title="Delete target" @click=${this.deleteTarget}><iconify-icon .icon=${ci} aria-hidden="true"></iconify-icon></button>
              <button class=${`button ${e.paused?"success":"warning"} icon-button`} type="button" aria-label=${e.paused?"Resume evaluations":"Pause evaluations"} title=${e.paused?"Resume evaluations":"Pause evaluations"} @click=${()=>this.setPaused(!e.paused)}><iconify-icon .icon=${e.paused?Ve:Be} aria-hidden="true"></iconify-icon></button>
            </div>
            <button class="button" type="submit" aria-busy=${this.saving?"true":"false"} ?disabled=${this.saving||!this.detailDirty}>Save changes</button>
          </div>
        </form>
        <section class="history">
          <div class="history-head"><h3>Evaluation history</h3>${i.length?h`<span class="meta">Latest ${i.length}</span>`:f}</div>
          ${i.length?h`
                <div class="chart-plot">
                  <div class="chart-scale" aria-hidden="true"><span>${r(n)}</span><span>${r(n/2)}</span><span>0 ms</span></div>
                  <div class="history-chart" role="list" aria-label=${`Recent evaluation latency, 0 to ${r(n)}`}>
                    ${i.map(o=>{const a=o.succeeded?"Passed":"Failed",l=o.status_code===null?"network error":`HTTP ${o.status_code}`,c=`${a} at ${new Date(o.recorded_at_ms).toLocaleString()}: ${o.latency_ms} ms, ${l}`;return h`<span class="history-bar ${o.succeeded?"up":"down"}" role="listitem" aria-label=${c} title=${c} style=${`height: ${Math.max(8,o.latency_ms/n*100)}%`}></span>`})}
                  </div>
                </div>
                <div class="chart-axis"><span>${s(i[0].recorded_at_ms)}</span><span>${s(i.at(-1).recorded_at_ms)}</span></div>
                <div class="chart-legend"><span><i class="up"></i>Passed</span><span><i class="down"></i>Failed</span><span>Height = latency</span></div>
              `:h`<p class="meta">No evaluations recorded yet.</p>`}
        </section>
      </dialog>
    `}};g.styles=Dt`
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
      display: block;
      min-height: 100vh;
      background: var(--page-background);
      color: var(--text);
      font: 14px/1.5 Inter, ui-sans-serif, system-ui, sans-serif;
      transition: background 220ms ease, color 180ms ease;
    }
    * { box-sizing: border-box; }
    button, input, select { font: inherit; }
    .shell { max-width: 1200px; margin: auto; padding: 28px 24px 72px; }
    header { display: grid; grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr); align-items: center; margin-bottom: 34px; }
    .brand, .actions, .live, nav { display: flex; align-items: center; }
    .brand { gap: 13px; }
    header > .brand { justify-self: start; }
    header > nav { justify-self: center; }
    header > .actions { justify-self: end; }
    .brand-line { display: flex; align-items: center; gap: 12px; }
    .brand img { width: 42px; height: 42px; filter: drop-shadow(0 0 18px var(--brand-shadow)); }
    .brand strong { display: block; font-size: 19px; letter-spacing: .02em; }
    .brand span, .live, .eyebrow, .meta { color: var(--muted); font-size: 12px; }
    nav { gap: 4px; padding: 4px; border: 1px solid var(--line); border-radius: 11px; background: var(--nav-bg); }
    nav a { color: var(--muted); padding: 7px 11px; text-decoration: none; border-radius: 7px; transition: background-color 160ms ease, color 160ms ease; }
    nav a.active { color: var(--text); background: var(--active-bg); }
    .actions { gap: 12px; }
    .live { gap: 7px; }
    .dot { width: 7px; height: 7px; border-radius: 50%; background: var(--amber); transition: background-color 160ms ease, box-shadow 160ms ease; }
    .dot.on { background: var(--green); box-shadow: 0 0 10px var(--green); }
    .heading { display: flex; align-items: flex-end; justify-content: space-between; margin-bottom: 18px; }
    .heading h1 { margin: 2px 0 0; font-size: clamp(27px, 4vw, 38px); line-height: 1.1; letter-spacing: -.035em; }
    .eyebrow { text-transform: uppercase; letter-spacing: .16em; }
    .button { border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; cursor: pointer; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    .button:hover { border-color: var(--button-hover-border); }
    .button:active { transform: translateY(1px); }
    .button:disabled { cursor: not-allowed; opacity: .65; }
    .button[aria-busy="true"] { cursor: wait; }
    .icon-button { display: grid; width: 36px; height: 36px; place-items: center; padding: 0; }
    iconify-icon { display: inline-block; width: 18px; height: 18px; font-size: 18px; }
    .summary { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }
    .metric, .panel { border: 1px solid var(--line); background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); transition: background-color 180ms ease, border-color 180ms ease, box-shadow 180ms ease; }
    .metric { border-radius: 14px; padding: 17px 18px; }
    .metric span { display: block; color: var(--muted); font-size: 11px; letter-spacing: .11em; text-transform: uppercase; }
    .metric strong { display: block; margin-top: 5px; font-size: 29px; font-weight: 560; }
    .panel { border-radius: 16px; overflow: hidden; }
    .resources { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; margin-top: 18px; }
    .resource { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 13px 20px; border-bottom: 1px solid var(--divider); }
    .resource:last-child { border-bottom: 0; }
    .resource strong { display: block; font-size: 13px; }
    .resource code { color: var(--muted); font-size: 11px; }
    .badge { border: 1px solid var(--badge-border); border-radius: 999px; color: var(--badge-text); padding: 2px 7px; font-size: 10px; text-transform: uppercase; }
    .panel-head { display: flex; align-items: center; justify-content: space-between; padding: 17px 20px; border-bottom: 1px solid var(--line); }
    .panel-head h2 { margin: 0; font-size: 14px; }
    .target-wrap { display: grid; grid-template-columns: auto minmax(0, 1fr); align-items: center; border-bottom: 1px solid var(--divider); padding-left: 20px; }
    .target-wrap:last-child { border-bottom: 0; }
    .select-target { width: 15px; height: 15px; accent-color: var(--green); }
    .target { width: 100%; display: grid; grid-template-columns: auto minmax(0, 1fr) auto; gap: 14px; align-items: center; padding: 17px 20px 17px 14px; border: 0; background: transparent; color: var(--text); text-align: left; cursor: pointer; }
    .target-wrap, .target { transition: background-color 150ms ease; }
    .target-wrap:hover, .target-wrap:hover .target { background: var(--row-hover); }
    .state { width: 10px; height: 10px; border-radius: 50%; background: var(--amber); box-shadow: 0 0 12px currentColor; transition: background-color 160ms ease, color 160ms ease, box-shadow 160ms ease; }
    .state.up { color: var(--green); background: var(--green); }
    .state.down { color: var(--red); background: var(--red); }
    .state.paused { color: var(--muted); background: var(--muted); box-shadow: none; }
    .target h3 { margin: 0 0 3px; font-size: 14px; }
    .meta { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .latency { text-align: right; }
    .latency strong { display: block; font-weight: 500; }
    .latency span { color: var(--muted); font-size: 11px; }
    .target-side { display: flex; align-items: center; gap: 20px; }
    .mini-chart { display: flex; width: 88px; height: 32px; align-items: flex-end; gap: 2px; }
    .mini-bar { flex: 1; min-width: 2px; max-width: 7px; border-radius: 2px 2px 1px 1px; opacity: .75; transition: background-color 160ms ease, height 180ms ease, opacity 160ms ease; }
    .mini-bar.up { background: var(--green); }
    .mini-bar.down { background: var(--red); }
    .empty { padding: 54px 20px; color: var(--muted); text-align: center; }
    .notice { margin: 0 0 16px; border: 1px solid var(--notice-border); border-radius: 10px; background: var(--notice-bg); color: var(--notice-text); padding: 10px 12px; }
    .toolbar { display: grid; grid-template-columns: minmax(180px, 1fr) auto auto; gap: 8px; padding: 12px 20px; border-bottom: 1px solid var(--line); }
    .toolbar input, .toolbar select { padding: 7px 9px; }
    .bulk { display: flex; align-items: center; gap: 8px; padding: 10px 20px; border-bottom: 1px solid var(--line); background: var(--bulk-bg); }
    .bulk-actions { display: flex; align-items: center; gap: 8px; margin-left: auto; }
    .bulk, .bulk-actions .button { animation: reveal 160ms ease-out; }
    @keyframes reveal { from { opacity: 0; transform: translateY(-3px); } }
    dialog { width: min(580px, calc(100% - 28px)); border: 1px solid var(--line); border-radius: 17px; background: var(--panel); color: var(--text); padding: 0; box-shadow: 0 28px 90px var(--dialog-shadow); opacity: 0; transform: translateY(8px) scale(.985); transition: opacity 170ms ease, transform 170ms ease, overlay 170ms allow-discrete, display 170ms allow-discrete; }
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
    form { display: grid; gap: 13px; padding: 20px 22px 22px; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    label { display: grid; gap: 5px; color: var(--muted); font-size: 11px; letter-spacing: .03em; }
    input, select { width: 100%; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease; }
    input:focus, select:focus { border-color: var(--focus); }
    input:disabled { cursor: not-allowed; opacity: .5; }
    .dialog-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 5px; }
    .danger-actions { display: flex; gap: 8px; margin-right: auto; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .danger { background: transparent; color: var(--danger-text); border-color: var(--danger-border); }
    .warning { background: transparent; color: var(--warning-text); border-color: var(--warning-border); }
    .warning:hover { border-color: var(--warning-text); }
    .success { background: transparent; color: var(--green); border-color: var(--green); }
    .success:hover { border-color: var(--button-text); }
    .dialog-close { position: absolute; top: 12px; right: 14px; }
    .check { display: flex; align-items: center; gap: 8px; }
    .check input { width: auto; }
    .history { margin: 0 22px 22px; border-top: 1px solid var(--line); padding-top: 18px; }
    .history-head, .chart-legend, .chart-legend span, .chart-axis { display: flex; align-items: center; }
    .history-head { justify-content: space-between; margin-bottom: 12px; }
    .history-head h3 { margin: 0; font-size: 14px; }
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
    .join-command { margin: 20px 22px; border: 1px solid var(--line); border-radius: 10px; background: var(--join-bg); color: var(--green); padding: 13px; overflow-wrap: anywhere; font: 12px/1.6 ui-monospace, SFMono-Regular, monospace; }
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
      :host, nav a, .button, .metric, .panel, .target-wrap, .target, .dot, .state, .mini-bar, .history-bar, dialog, dialog::backdrop, input, select { transition-duration: 0s; }
      .bulk, .bulk-actions .button { animation-duration: 0s; }
    }
    @media (max-width: 720px) {
      .shell { padding: 20px 14px 60px; }
      header { grid-template-columns: minmax(0, 1fr) auto; }
      nav { display: none; }
      .summary { grid-template-columns: 1fr 1fr; }
      .resources { grid-template-columns: 1fr; }
      .toolbar { grid-template-columns: 1fr 1fr; }
      .toolbar input { grid-column: 1 / -1; }
      .heading { align-items: flex-start; gap: 16px; }
      .target { grid-template-columns: auto minmax(0, 1fr); }
      .target-side { grid-column: 2; justify-self: start; }
      .latency { text-align: left; }
    }
  `;b([v()],g.prototype,"targets",2);b([v()],g.prototype,"channels",2);b([v()],g.prototype,"alerts",2);b([v()],g.prototype,"secrets",2);b([v()],g.prototype,"cluster",2);b([v()],g.prototype,"joinTokens",2);b([v()],g.prototype,"error",2);b([v()],g.prototype,"live",2);b([v()],g.prototype,"saving",2);b([v()],g.prototype,"selected",2);b([v()],g.prototype,"channelKind",2);b([v()],g.prototype,"joinCommand",2);b([v()],g.prototype,"search",2);b([v()],g.prototype,"statusFilter",2);b([v()],g.prototype,"sort",2);b([v()],g.prototype,"selectedIds",2);b([v()],g.prototype,"activeSection",2);b([v()],g.prototype,"copied",2);b([v()],g.prototype,"setupMode",2);b([v()],g.prototype,"joining",2);b([v()],g.prototype,"unlimitedUses",2);b([v()],g.prototype,"theme",2);b([v()],g.prototype,"detailDirty",2);g=b([ii("upgrid-app")],g);
