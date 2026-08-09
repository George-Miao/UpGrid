(function(){const e=document.createElement("link").relList;if(e&&e.supports&&e.supports("modulepreload"))return;for(const s of document.querySelectorAll('link[rel="modulepreload"]'))r(s);new MutationObserver(s=>{for(const a of s)if(a.type==="childList")for(const o of a.addedNodes)o.tagName==="LINK"&&o.rel==="modulepreload"&&r(o)}).observe(document,{childList:!0,subtree:!0});function t(s){const a={};return s.integrity&&(a.integrity=s.integrity),s.referrerPolicy&&(a.referrerPolicy=s.referrerPolicy),s.crossOrigin==="use-credentials"?a.credentials="include":s.crossOrigin==="anonymous"?a.credentials="omit":a.credentials="same-origin",a}function r(s){if(s.ep)return;s.ep=!0;const a=t(s);fetch(s.href,a)}})();const R=globalThis,z=R.ShadowRoot&&(R.ShadyCSS===void 0||R.ShadyCSS.nativeShadow)&&"adoptedStyleSheets"in Document.prototype&&"replace"in CSSStyleSheet.prototype,I=Symbol(),K=new WeakMap;let re=class{constructor(e,t,r){if(this._$cssResult$=!0,r!==I)throw Error("CSSResult is not constructable. Use `unsafeCSS` or `css` instead.");this.cssText=e,this.t=t}get styleSheet(){let e=this.o;const t=this.t;if(z&&e===void 0){const r=t!==void 0&&t.length===1;r&&(e=K.get(t)),e===void 0&&((this.o=e=new CSSStyleSheet).replaceSync(this.cssText),r&&K.set(t,e))}return e}toString(){return this.cssText}};const de=i=>new re(typeof i=="string"?i:i+"",void 0,I),ce=(i,...e)=>{const t=i.length===1?i[0]:e.reduce((r,s,a)=>r+(o=>{if(o._$cssResult$===!0)return o.cssText;if(typeof o=="number")return o;throw Error("Value passed to 'css' function must be a 'css' function result: "+o+". Use 'unsafeCSS' to pass non-literal values, but take care to ensure page security.")})(s)+i[a+1],i[0]);return new re(t,i,I)},he=(i,e)=>{if(z)i.adoptedStyleSheets=e.map(t=>t instanceof CSSStyleSheet?t:t.styleSheet);else for(const t of e){const r=document.createElement("style"),s=R.litNonce;s!==void 0&&r.setAttribute("nonce",s),r.textContent=t.cssText,i.appendChild(r)}},V=z?i=>i:i=>i instanceof CSSStyleSheet?(e=>{let t="";for(const r of e.cssRules)t+=r.cssText;return de(t)})(i):i;const{is:pe,defineProperty:ue,getOwnPropertyDescriptor:ge,getOwnPropertyNames:me,getOwnPropertySymbols:be,getPrototypeOf:ve}=Object,L=globalThis,Z=L.trustedTypes,fe=Z?Z.emptyScript:"",$e=L.reactiveElementPolyfillSupport,C=(i,e)=>i,j={toAttribute(i,e){switch(e){case Boolean:i=i?fe:null;break;case Object:case Array:i=i==null?i:JSON.stringify(i)}return i},fromAttribute(i,e){let t=i;switch(e){case Boolean:t=i!==null;break;case Number:t=i===null?null:Number(i);break;case Object:case Array:try{t=JSON.parse(i)}catch{t=null}}return t}},B=(i,e)=>!pe(i,e),G={attribute:!0,type:String,converter:j,reflect:!1,useDefault:!1,hasChanged:B};Symbol.metadata??=Symbol("metadata"),L.litPropertyMetadata??=new WeakMap;let S=class extends HTMLElement{static addInitializer(e){this._$Ei(),(this.l??=[]).push(e)}static get observedAttributes(){return this.finalize(),this._$Eh&&[...this._$Eh.keys()]}static createProperty(e,t=G){if(t.state&&(t.attribute=!1),this._$Ei(),this.prototype.hasOwnProperty(e)&&((t=Object.create(t)).wrapped=!0),this.elementProperties.set(e,t),!t.noAccessor){const r=Symbol(),s=this.getPropertyDescriptor(e,r,t);s!==void 0&&ue(this.prototype,e,s)}}static getPropertyDescriptor(e,t,r){const{get:s,set:a}=ge(this.prototype,e)??{get(){return this[t]},set(o){this[t]=o}};return{get:s,set(o){const l=s?.call(this);a?.call(this,o),this.requestUpdate(e,l,r)},configurable:!0,enumerable:!0}}static getPropertyOptions(e){return this.elementProperties.get(e)??G}static _$Ei(){if(this.hasOwnProperty(C("elementProperties")))return;const e=ve(this);e.finalize(),e.l!==void 0&&(this.l=[...e.l]),this.elementProperties=new Map(e.elementProperties)}static finalize(){if(this.hasOwnProperty(C("finalized")))return;if(this.finalized=!0,this._$Ei(),this.hasOwnProperty(C("properties"))){const t=this.properties,r=[...me(t),...be(t)];for(const s of r)this.createProperty(s,t[s])}const e=this[Symbol.metadata];if(e!==null){const t=litPropertyMetadata.get(e);if(t!==void 0)for(const[r,s]of t)this.elementProperties.set(r,s)}this._$Eh=new Map;for(const[t,r]of this.elementProperties){const s=this._$Eu(t,r);s!==void 0&&this._$Eh.set(s,t)}this.elementStyles=this.finalizeStyles(this.styles)}static finalizeStyles(e){const t=[];if(Array.isArray(e)){const r=new Set(e.flat(1/0).reverse());for(const s of r)t.unshift(V(s))}else e!==void 0&&t.push(V(e));return t}static _$Eu(e,t){const r=t.attribute;return r===!1?void 0:typeof r=="string"?r:typeof e=="string"?e.toLowerCase():void 0}constructor(){super(),this._$Ep=void 0,this.isUpdatePending=!1,this.hasUpdated=!1,this._$Em=null,this._$Ev()}_$Ev(){this._$ES=new Promise(e=>this.enableUpdating=e),this._$AL=new Map,this._$E_(),this.requestUpdate(),this.constructor.l?.forEach(e=>e(this))}addController(e){(this._$EO??=new Set).add(e),this.renderRoot!==void 0&&this.isConnected&&e.hostConnected?.()}removeController(e){this._$EO?.delete(e)}_$E_(){const e=new Map,t=this.constructor.elementProperties;for(const r of t.keys())this.hasOwnProperty(r)&&(e.set(r,this[r]),delete this[r]);e.size>0&&(this._$Ep=e)}createRenderRoot(){const e=this.shadowRoot??this.attachShadow(this.constructor.shadowRootOptions);return he(e,this.constructor.elementStyles),e}connectedCallback(){this.renderRoot??=this.createRenderRoot(),this.enableUpdating(!0),this._$EO?.forEach(e=>e.hostConnected?.())}enableUpdating(e){}disconnectedCallback(){this._$EO?.forEach(e=>e.hostDisconnected?.())}attributeChangedCallback(e,t,r){this._$AK(e,r)}_$ET(e,t){const r=this.constructor.elementProperties.get(e),s=this.constructor._$Eu(e,r);if(s!==void 0&&r.reflect===!0){const a=(r.converter?.toAttribute!==void 0?r.converter:j).toAttribute(t,r.type);this._$Em=e,a==null?this.removeAttribute(s):this.setAttribute(s,a),this._$Em=null}}_$AK(e,t){const r=this.constructor,s=r._$Eh.get(e);if(s!==void 0&&this._$Em!==s){const a=r.getPropertyOptions(s),o=typeof a.converter=="function"?{fromAttribute:a.converter}:a.converter?.fromAttribute!==void 0?a.converter:j;this._$Em=s;const l=o.fromAttribute(t,a.type);this[s]=l??this._$Ej?.get(s)??l,this._$Em=null}}requestUpdate(e,t,r,s=!1,a){if(e!==void 0){const o=this.constructor;if(s===!1&&(a=this[e]),r??=o.getPropertyOptions(e),!((r.hasChanged??B)(a,t)||r.useDefault&&r.reflect&&a===this._$Ej?.get(e)&&!this.hasAttribute(o._$Eu(e,r))))return;this.C(e,t,r)}this.isUpdatePending===!1&&(this._$ES=this._$EP())}C(e,t,{useDefault:r,reflect:s,wrapped:a},o){r&&!(this._$Ej??=new Map).has(e)&&(this._$Ej.set(e,o??t??this[e]),a!==!0||o!==void 0)||(this._$AL.has(e)||(this.hasUpdated||r||(t=void 0),this._$AL.set(e,t)),s===!0&&this._$Em!==e&&(this._$Eq??=new Set).add(e))}async _$EP(){this.isUpdatePending=!0;try{await this._$ES}catch(t){Promise.reject(t)}const e=this.scheduleUpdate();return e!=null&&await e,!this.isUpdatePending}scheduleUpdate(){return this.performUpdate()}performUpdate(){if(!this.isUpdatePending)return;if(!this.hasUpdated){if(this.renderRoot??=this.createRenderRoot(),this._$Ep){for(const[s,a]of this._$Ep)this[s]=a;this._$Ep=void 0}const r=this.constructor.elementProperties;if(r.size>0)for(const[s,a]of r){const{wrapped:o}=a,l=this[s];o!==!0||this._$AL.has(s)||l===void 0||this.C(s,void 0,a,l)}}let e=!1;const t=this._$AL;try{e=this.shouldUpdate(t),e?(this.willUpdate(t),this._$EO?.forEach(r=>r.hostUpdate?.()),this.update(t)):this._$EM()}catch(r){throw e=!1,this._$EM(),r}e&&this._$AE(t)}willUpdate(e){}_$AE(e){this._$EO?.forEach(t=>t.hostUpdated?.()),this.hasUpdated||(this.hasUpdated=!0,this.firstUpdated(e)),this.updated(e)}_$EM(){this._$AL=new Map,this.isUpdatePending=!1}get updateComplete(){return this.getUpdateComplete()}getUpdateComplete(){return this._$ES}shouldUpdate(e){return!0}update(e){this._$Eq&&=this._$Eq.forEach(t=>this._$ET(t,this[t])),this._$EM()}updated(e){}firstUpdated(e){}};S.elementStyles=[],S.shadowRootOptions={mode:"open"},S[C("elementProperties")]=new Map,S[C("finalized")]=new Map,$e?.({ReactiveElement:S}),(L.reactiveElementVersions??=[]).push("2.1.2");const F=globalThis,Q=i=>i,M=F.trustedTypes,X=M?M.createPolicy("lit-html",{createHTML:i=>i}):void 0,ae="$lit$",y=`lit$${Math.random().toFixed(9).slice(2)}$`,oe="?"+y,ye=`<${oe}>`,w=document,P=()=>w.createComment(""),D=i=>i===null||typeof i!="object"&&typeof i!="function",J=Array.isArray,_e=i=>J(i)||typeof i?.[Symbol.iterator]=="function",q=`[ 	
\f\r]`,E=/<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g,Y=/-->/g,ee=/>/g,_=RegExp(`>|${q}(?:([^\\s"'>=/]+)(${q}*=${q}*(?:[^ 	
\f\r"'\`<>=]|("|')|))|$)`,"g"),te=/'/g,se=/"/g,ne=/^(?:script|style|textarea|title)$/i,xe=i=>(e,...t)=>({_$litType$:i,strings:e,values:t}),h=xe(1),A=Symbol.for("lit-noChange"),c=Symbol.for("lit-nothing"),ie=new WeakMap,x=w.createTreeWalker(w,129);function le(i,e){if(!J(i)||!i.hasOwnProperty("raw"))throw Error("invalid template strings array");return X!==void 0?X.createHTML(e):e}const we=(i,e)=>{const t=i.length-1,r=[];let s,a=e===2?"<svg>":e===3?"<math>":"",o=E;for(let l=0;l<t;l++){const n=i[l];let u,b,d=-1,f=0;for(;f<n.length&&(o.lastIndex=f,b=o.exec(n),b!==null);)f=o.lastIndex,o===E?b[1]==="!--"?o=Y:b[1]!==void 0?o=ee:b[2]!==void 0?(ne.test(b[2])&&(s=RegExp("</"+b[2],"g")),o=_):b[3]!==void 0&&(o=_):o===_?b[0]===">"?(o=s??E,d=-1):b[1]===void 0?d=-2:(d=o.lastIndex-b[2].length,u=b[1],o=b[3]===void 0?_:b[3]==='"'?se:te):o===se||o===te?o=_:o===Y||o===ee?o=E:(o=_,s=void 0);const $=o===_&&i[l+1].startsWith("/>")?" ":"";a+=o===E?n+ye:d>=0?(r.push(u),n.slice(0,d)+ae+n.slice(d)+y+$):n+y+(d===-2?l:$)}return[le(i,a+(i[t]||"<?>")+(e===2?"</svg>":e===3?"</math>":"")),r]};class O{constructor({strings:e,_$litType$:t},r){let s;this.parts=[];let a=0,o=0;const l=e.length-1,n=this.parts,[u,b]=we(e,t);if(this.el=O.createElement(u,r),x.currentNode=this.el.content,t===2||t===3){const d=this.el.content.firstChild;d.replaceWith(...d.childNodes)}for(;(s=x.nextNode())!==null&&n.length<l;){if(s.nodeType===1){if(s.hasAttributes())for(const d of s.getAttributeNames())if(d.endsWith(ae)){const f=b[o++],$=s.getAttribute(d).split(y),U=/([.?@])?(.*)/.exec(f);n.push({type:1,index:a,name:U[2],strings:$,ctor:U[1]==="."?Ae:U[1]==="?"?ke:U[1]==="@"?Ee:H}),s.removeAttribute(d)}else d.startsWith(y)&&(n.push({type:6,index:a}),s.removeAttribute(d));if(ne.test(s.tagName)){const d=s.textContent.split(y),f=d.length-1;if(f>0){s.textContent=M?M.emptyScript:"";for(let $=0;$<f;$++)s.append(d[$],P()),x.nextNode(),n.push({type:2,index:++a});s.append(d[f],P())}}}else if(s.nodeType===8)if(s.data===oe)n.push({type:2,index:a});else{let d=-1;for(;(d=s.data.indexOf(y,d+1))!==-1;)n.push({type:7,index:a}),d+=y.length-1}a++}}static createElement(e,t){const r=w.createElement("template");return r.innerHTML=e,r}}function k(i,e,t=i,r){if(e===A)return e;let s=r!==void 0?t._$Co?.[r]:t._$Cl;const a=D(e)?void 0:e._$litDirective$;return s?.constructor!==a&&(s?._$AO?.(!1),a===void 0?s=void 0:(s=new a(i),s._$AT(i,t,r)),r!==void 0?(t._$Co??=[])[r]=s:t._$Cl=s),s!==void 0&&(e=k(i,s._$AS(i,e.values),s,r)),e}class Se{constructor(e,t){this._$AV=[],this._$AN=void 0,this._$AD=e,this._$AM=t}get parentNode(){return this._$AM.parentNode}get _$AU(){return this._$AM._$AU}u(e){const{el:{content:t},parts:r}=this._$AD,s=(e?.creationScope??w).importNode(t,!0);x.currentNode=s;let a=x.nextNode(),o=0,l=0,n=r[0];for(;n!==void 0;){if(o===n.index){let u;n.type===2?u=new N(a,a.nextSibling,this,e):n.type===1?u=new n.ctor(a,n.name,n.strings,this,e):n.type===6&&(u=new Ce(a,this,e)),this._$AV.push(u),n=r[++l]}o!==n?.index&&(a=x.nextNode(),o++)}return x.currentNode=w,s}p(e){let t=0;for(const r of this._$AV)r!==void 0&&(r.strings!==void 0?(r._$AI(e,r,t),t+=r.strings.length-2):r._$AI(e[t])),t++}}class N{get _$AU(){return this._$AM?._$AU??this._$Cv}constructor(e,t,r,s){this.type=2,this._$AH=c,this._$AN=void 0,this._$AA=e,this._$AB=t,this._$AM=r,this.options=s,this._$Cv=s?.isConnected??!0}get parentNode(){let e=this._$AA.parentNode;const t=this._$AM;return t!==void 0&&e?.nodeType===11&&(e=t.parentNode),e}get startNode(){return this._$AA}get endNode(){return this._$AB}_$AI(e,t=this){e=k(this,e,t),D(e)?e===c||e==null||e===""?(this._$AH!==c&&this._$AR(),this._$AH=c):e!==this._$AH&&e!==A&&this._(e):e._$litType$!==void 0?this.$(e):e.nodeType!==void 0?this.T(e):_e(e)?this.k(e):this._(e)}O(e){return this._$AA.parentNode.insertBefore(e,this._$AB)}T(e){this._$AH!==e&&(this._$AR(),this._$AH=this.O(e))}_(e){this._$AH!==c&&D(this._$AH)?this._$AA.nextSibling.data=e:this.T(w.createTextNode(e)),this._$AH=e}$(e){const{values:t,_$litType$:r}=e,s=typeof r=="number"?this._$AC(e):(r.el===void 0&&(r.el=O.createElement(le(r.h,r.h[0]),this.options)),r);if(this._$AH?._$AD===s)this._$AH.p(t);else{const a=new Se(s,this),o=a.u(this.options);a.p(t),this.T(o),this._$AH=a}}_$AC(e){let t=ie.get(e.strings);return t===void 0&&ie.set(e.strings,t=new O(e)),t}k(e){J(this._$AH)||(this._$AH=[],this._$AR());const t=this._$AH;let r,s=0;for(const a of e)s===t.length?t.push(r=new N(this.O(P()),this.O(P()),this,this.options)):r=t[s],r._$AI(a),s++;s<t.length&&(this._$AR(r&&r._$AB.nextSibling,s),t.length=s)}_$AR(e=this._$AA.nextSibling,t){for(this._$AP?.(!1,!0,t);e!==this._$AB;){const r=Q(e).nextSibling;Q(e).remove(),e=r}}setConnected(e){this._$AM===void 0&&(this._$Cv=e,this._$AP?.(e))}}class H{get tagName(){return this.element.tagName}get _$AU(){return this._$AM._$AU}constructor(e,t,r,s,a){this.type=1,this._$AH=c,this._$AN=void 0,this.element=e,this.name=t,this._$AM=s,this.options=a,r.length>2||r[0]!==""||r[1]!==""?(this._$AH=Array(r.length-1).fill(new String),this.strings=r):this._$AH=c}_$AI(e,t=this,r,s){const a=this.strings;let o=!1;if(a===void 0)e=k(this,e,t,0),o=!D(e)||e!==this._$AH&&e!==A,o&&(this._$AH=e);else{const l=e;let n,u;for(e=a[0],n=0;n<a.length-1;n++)u=k(this,l[r+n],t,n),u===A&&(u=this._$AH[n]),o||=!D(u)||u!==this._$AH[n],u===c?e=c:e!==c&&(e+=(u??"")+a[n+1]),this._$AH[n]=u}o&&!s&&this.j(e)}j(e){e===c?this.element.removeAttribute(this.name):this.element.setAttribute(this.name,e??"")}}class Ae extends H{constructor(){super(...arguments),this.type=3}j(e){this.element[this.name]=e===c?void 0:e}}class ke extends H{constructor(){super(...arguments),this.type=4}j(e){this.element.toggleAttribute(this.name,!!e&&e!==c)}}class Ee extends H{constructor(e,t,r,s,a){super(e,t,r,s,a),this.type=5}_$AI(e,t=this){if((e=k(this,e,t,0)??c)===A)return;const r=this._$AH,s=e===c&&r!==c||e.capture!==r.capture||e.once!==r.once||e.passive!==r.passive,a=e!==c&&(r===c||s);s&&this.element.removeEventListener(this.name,this,r),a&&this.element.addEventListener(this.name,this,e),this._$AH=e}handleEvent(e){typeof this._$AH=="function"?this._$AH.call(this.options?.host??this.element,e):this._$AH.handleEvent(e)}}class Ce{constructor(e,t,r){this.element=e,this.type=6,this._$AN=void 0,this._$AM=t,this.options=r}get _$AU(){return this._$AM._$AU}_$AI(e){k(this,e)}}const Te=F.litHtmlPolyfillSupport;Te?.(O,N),(F.litHtmlVersions??=[]).push("3.3.3");const Pe=(i,e,t)=>{const r=t?.renderBefore??e;let s=r._$litPart$;if(s===void 0){const a=t?.renderBefore??null;r._$litPart$=s=new N(e.insertBefore(P(),a),a,void 0,t??{})}return s._$AI(i),s};const W=globalThis;class T extends S{constructor(){super(...arguments),this.renderOptions={host:this},this._$Do=void 0}createRenderRoot(){const e=super.createRenderRoot();return this.renderOptions.renderBefore??=e.firstChild,e}update(e){const t=this.render();this.hasUpdated||(this.renderOptions.isConnected=this.isConnected),super.update(e),this._$Do=Pe(t,this.renderRoot,this.renderOptions)}connectedCallback(){super.connectedCallback(),this._$Do?.setConnected(!0)}disconnectedCallback(){super.disconnectedCallback(),this._$Do?.setConnected(!1)}render(){return A}}T._$litElement$=!0,T.finalized=!0,W.litElementHydrateSupport?.({LitElement:T});const De=W.litElementPolyfillSupport;De?.({LitElement:T});(W.litElementVersions??=[]).push("4.2.2");const Oe=i=>(e,t)=>{t!==void 0?t.addInitializer(()=>{customElements.define(i,e)}):customElements.define(i,e)};const Ne={attribute:!0,type:String,converter:j,reflect:!1,hasChanged:B},Ue=(i=Ne,e,t)=>{const{kind:r,metadata:s}=t;let a=globalThis.litPropertyMetadata.get(s);if(a===void 0&&globalThis.litPropertyMetadata.set(s,a=new Map),r==="setter"&&((i=Object.create(i)).wrapped=!0),a.set(t.name,i),r==="accessor"){const{name:o}=t;return{set(l){const n=e.get.call(this);e.set.call(this,l),this.requestUpdate(o,n,i,!0,l)},init(l){return l!==void 0&&this.C(o,void 0,i,l),l}}}if(r==="setter"){const{name:o}=t;return function(l){const n=this[o];e.call(this,l),this.requestUpdate(o,n,i,!0,l)}}throw Error("Unsupported decorator location: "+r)};function Re(i){return(e,t)=>typeof t=="object"?Ue(i,e,t):((r,s,a)=>{const o=s.hasOwnProperty(a);return s.constructor.createProperty(a,r),o?Object.getOwnPropertyDescriptor(s,a):void 0})(i,e,t)}function m(i){return Re({...i,state:!0,attribute:!1})}async function v(i,e){const t=await fetch(i,{...e,headers:{...e?.body?{"content-type":"application/json"}:{},...e?.headers}});if(!t.ok){const r=await t.json().catch(()=>({error:t.statusText}));throw new Error(r.error||t.statusText)}return t.status===204?void 0:t.json()}var je=Object.defineProperty,Me=Object.getOwnPropertyDescriptor,g=(i,e,t,r)=>{for(var s=r>1?void 0:r?Me(e,t):e,a=i.length-1,o;a>=0;a--)(o=i[a])&&(s=(r?o(e,t,s):o(s))||s);return r&&s&&je(e,t,s),s};let p=class extends T{constructor(){super(...arguments),this.targets=[],this.channels=[],this.alerts=[],this.secrets=[],this.error="",this.live=!1,this.saving=!1,this.channelKind="webhook",this.joinCommand="",this.search="",this.statusFilter="all",this.sort="name",this.selectedIds=new Set,this.activeSection="overview",this.copied=!1}connectedCallback(){super.connectedCallback(),this.refresh(),this.events=new EventSource("/api/v1/events"),this.events.addEventListener("state",()=>{this.refresh()}),this.events.onopen=()=>this.live=!0,this.events.onerror=()=>this.live=!1}disconnectedCallback(){this.events?.close(),super.disconnectedCallback()}async refresh(){try{[this.targets,this.channels,this.alerts,this.secrets,this.cluster]=await Promise.all([v("/api/v1/targets"),v("/api/v1/channels"),v("/api/v1/alerts"),v("/api/v1/secrets"),v("/api/v1/cluster")]),this.error=""}catch(i){this.error=i instanceof Error?i.message:String(i)}}openTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.showModal()}closeTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.close()}openTarget(i){this.selected=i,this.updateComplete.then(()=>this.renderRoot.querySelector("#detail-dialog")?.showModal())}closeDetailDialog(){this.renderRoot.querySelector("#detail-dialog")?.close(),this.selected=void 0}showDialog(i){this.renderRoot.querySelector(`#${i}`)?.showModal()}dismissOnBackdrop(i){const e=i.currentTarget;i.target===e&&(e.close(),e.id==="detail-dialog"&&(this.selected=void 0))}navigate(i,e){i.preventDefault(),this.activeSection=e,this.renderRoot.querySelector(`#${e}`)?.scrollIntoView({behavior:"smooth",block:"start"})}closeDialog(i){this.renderRoot.querySelector(`#${i}`)?.close()}async createTarget(i){i.preventDefault();const e=i.currentTarget,t=new FormData(e),r={name:String(t.get("name")),url:String(t.get("url")),method:String(t.get("method")),accepted_statuses:[{start:200,end:299}],follow_redirects:!0,max_redirects:5,interval_seconds:Number(t.get("interval")),timeout_seconds:Number(t.get("timeout")),failure_threshold:Number(t.get("failures")),headers:{},body:null,body_contains:null,skip_tls_verification:!1,notification_channel_ids:[]};this.saving=!0;try{await v("/api/v1/targets",{method:"POST",body:JSON.stringify(r)}),e.reset(),this.closeTargetDialog(),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}async updateTarget(i){if(i.preventDefault(),!this.selected)return;const e=i.currentTarget,t=new FormData(e),r=String(t.get("statuses")).split(",").map(a=>{const[o,l]=a.trim().split("-").map(Number);return{start:o,end:l||o}}),s={name:String(t.get("name")),url:String(t.get("url")),method:String(t.get("method")),accepted_statuses:r,follow_redirects:t.get("follow_redirects")==="on",max_redirects:Number(t.get("max_redirects")),interval_seconds:Number(t.get("interval")),timeout_seconds:Number(t.get("timeout")),failure_threshold:Number(t.get("failures")),headers:Object.fromEntries(Object.entries(this.selected.headers).map(([a,o])=>[a,o.kind==="literal"?o.value:{secret_id:o.secret_id}])),body:this.selected.body?.kind==="literal"?this.selected.body.value:this.selected.body?{secret_id:this.selected.body.secret_id}:null,body_contains:String(t.get("body_contains"))||null,skip_tls_verification:t.get("skip_tls_verification")==="on",notification_channel_ids:this.selected.notification_channel_ids};this.saving=!0;try{await v(`/api/v1/targets/${this.selected.id}`,{method:"PUT",body:JSON.stringify(s)}),this.closeDetailDialog(),await this.refresh()}catch(a){this.error=a instanceof Error?a.message:String(a)}finally{this.saving=!1}}async deleteTarget(){if(!(!this.selected||!window.confirm("Delete this target and its history?"))){this.saving=!0;try{await v(`/api/v1/targets/${this.selected.id}`,{method:"DELETE"}),this.closeDetailDialog(),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}}async setPaused(i){if(this.selected){this.saving=!0;try{await v(`/api/v1/targets/${this.selected.id}/${i?"pause":"resume"}`,{method:"POST"}),this.closeDetailDialog(),await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async createSecret(i){i.preventDefault();const e=i.currentTarget,t=new FormData(e);this.saving=!0;try{await v("/api/v1/secrets",{method:"POST",body:JSON.stringify({name:t.get("name"),value:t.get("value")})}),e.reset(),this.closeDialog("secret-dialog"),await this.refresh()}catch(r){this.error=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}async createChannel(i){i.preventDefault();const e=i.currentTarget,t=new FormData(e),r=this.channelKind==="telegram"?{type:"telegram",name:t.get("name"),bot_token:t.get("bot_token"),chat_id:t.get("chat_id")}:{type:"webhook",name:t.get("name"),url:t.get("url"),headers:{}};this.saving=!0;try{await v("/api/v1/channels",{method:"POST",body:JSON.stringify(r)}),e.reset(),this.channelKind="webhook",this.closeDialog("channel-dialog"),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}async createJoinLink(){this.saving=!0;try{const i=await v("/api/v1/join-links",{method:"POST",body:JSON.stringify({expires_in_seconds:600})});this.joinCommand=`upgrid --join '${i.url}'`,this.copied=!1,this.showDialog("join-dialog")}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}async copyJoinCommand(){let i=!1;try{await navigator.clipboard.writeText(this.joinCommand),i=!0}catch{const e=document.createElement("textarea");e.value=this.joinCommand,e.style.position="fixed",e.style.opacity="0",document.body.append(e),e.select(),i=document.execCommand("copy"),e.remove()}if(!i){this.error="Could not copy the Join command";return}this.copied=!0,window.setTimeout(()=>this.copied=!1,2e3)}toggleSelected(i,e){const t=new Set(this.selectedIds);e?t.add(i):t.delete(i),this.selectedIds=t}async bulkPause(i){this.saving=!0;try{await Promise.all([...this.selectedIds].map(e=>v(`/api/v1/targets/${e}/${i?"pause":"resume"}`,{method:"POST"}))),this.selectedIds=new Set,await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}async bulkDelete(){if(window.confirm(`Delete ${this.selectedIds.size} selected Targets and their history?`)){this.saving=!0;try{await Promise.all([...this.selectedIds].map(i=>v(`/api/v1/targets/${i}`,{method:"DELETE"}))),this.selectedIds=new Set,await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}}async deleteResource(i,e,t){if(window.confirm(`Delete ${t}?`))try{await v(`/api/v1/${i}/${e}`,{method:"DELETE"}),await this.refresh()}catch(r){this.error=r instanceof Error?r.message:String(r)}}render(){const i=this.targets.filter(s=>s.availability==="up").length,e=this.targets.filter(s=>s.availability==="down").length,t=this.alerts.filter(s=>s.delivery==="pending").length,r=this.targets.filter(s=>`${s.name} ${s.url}`.toLowerCase().includes(this.search.toLowerCase())).filter(s=>this.statusFilter==="all"?!0:this.statusFilter==="paused"?s.paused:s.availability===this.statusFilter).sort((s,a)=>this.sort==="status"&&s.availability.localeCompare(a.availability)||s.name.localeCompare(a.name));return h`
      <main class="shell">
        <header>
          <div class="brand">
            <img src="/favicon.svg" alt="" />
            <div><strong>UpGrid</strong><span>Distributed service monitoring</span></div>
          </div>
          <nav aria-label="Primary">
            ${["overview","targets","alerts","cluster"].map(s=>h`<a class=${this.activeSection===s?"active":""} href=${`#${s}`} @click=${a=>this.navigate(a,s)}>${s[0].toUpperCase()}${s.slice(1)}</a>`)}
          </nav>
          <div class="actions">
            <button class="button secondary" @click=${this.createJoinLink} ?disabled=${this.saving}>Add node</button>
            <div class="live"><i class="dot ${this.live?"on":""}"></i>${this.live?"live":"connecting"}</div>
          </div>
        </header>
        <section class="heading" id="overview">
          <div><span class="eyebrow">Cluster status</span><h1>Overview</h1></div>
          <button class="button" @click=${this.openTargetDialog}>Add target</button>
        </section>
        ${this.error?h`<div class="notice" role="alert">${this.error}</div>`:c}
        <section class="summary" aria-label="Target summary">
          <div class="metric"><span>Targets</span><strong>${this.targets.length}</strong></div>
          <div class="metric"><span>Up</span><strong>${i}</strong></div>
          <div class="metric"><span>Down</span><strong>${e}</strong></div>
          <div class="metric"><span>Pending alerts</span><strong>${t}</strong></div>
        </section>
        <section class="panel" id="targets">
          <div class="panel-head"><h2>Targets</h2><span class="meta">${this.targets.length} configured</span></div>
          <div class="toolbar">
            <input aria-label="Search targets" type="search" placeholder="Search name or URL" .value=${this.search} @input=${s=>this.search=s.target.value} />
            <select aria-label="Filter targets" .value=${this.statusFilter} @change=${s=>this.statusFilter=s.target.value}><option value="all">All states</option><option value="up">Up</option><option value="down">Down</option><option value="unknown">Unknown</option><option value="paused">Paused</option></select>
            <select aria-label="Sort targets" .value=${this.sort} @change=${s=>this.sort=s.target.value}><option value="name">Sort by name</option><option value="status">Sort by status</option></select>
          </div>
          ${this.selectedIds.size?h`<div class="bulk"><span class="meta">${this.selectedIds.size} selected</span><button class="button secondary" @click=${()=>this.bulkPause(!0)}>Pause selected</button><button class="button secondary" @click=${()=>this.bulkPause(!1)}>Resume selected</button><button class="button danger" @click=${this.bulkDelete}>Delete selected</button></div>`:c}
          ${r.length?r.map(s=>this.renderTarget(s)):h`<div class="empty">${this.targets.length?"No Targets match these filters.":"No targets yet. Add the first one to begin monitoring."}</div>`}
        </section>
        <section class="resources" aria-label="Notification configuration">
          <section class="panel">
            <div class="panel-head"><h2>Notification channels</h2><button class="button secondary" @click=${()=>this.showDialog("channel-dialog")}>Add channel</button></div>
            ${this.channels.length?this.channels.map(s=>h`<div class="resource"><div><strong>${s.name}</strong><code>${s.destination}</code></div><div class="actions"><span class="badge">${s.kind}</span><button class="button danger" aria-label=${`Delete channel ${s.name}`} @click=${()=>this.deleteResource("channels",s.id,s.name)}>Delete</button></div></div>`):h`<div class="empty">No notification channels.</div>`}
          </section>
          <section class="panel">
            <div class="panel-head"><h2>Secrets</h2><button class="button secondary" @click=${()=>this.showDialog("secret-dialog")}>Add secret</button></div>
            ${this.secrets.length?this.secrets.map(s=>h`<div class="resource"><div><strong>${s.name}</strong><code>${s.id}</code></div><div class="actions"><span class="badge">write-only</span><button class="button danger" aria-label=${`Delete secret ${s.name}`} @click=${()=>this.deleteResource("secrets",s.id,s.name)}>Delete</button></div></div>`):h`<div class="empty">No reusable Secrets.</div>`}
          </section>
          <section class="panel" id="alerts">
            <div class="panel-head"><h2>Alert history</h2><span class="meta">${this.alerts.length} events</span></div>
            ${this.alerts.length?this.alerts.slice(0,10).map(s=>h`<div class="resource"><div><strong>${s.target_name}</strong><code>${new Date(s.scheduled_at_ms).toLocaleString()}</code></div><span class="badge">${s.kind} · ${s.delivery}</span></div>`):h`<div class="empty">No availability transitions.</div>`}
          </section>
          <section class="panel" id="cluster" aria-label="Cluster topology">
            <div class="panel-head"><h2>Cluster access</h2><button class="button secondary" @click=${this.createJoinLink}>Add node</button></div>
            ${this.cluster?.members.map(s=>h`<div class="resource"><div><strong>${s.raft_url}</strong><code>${s.id}</code></div><div class="actions">${s.local?h`<span class="badge">This node</span>`:c}${s.leader?h`<span class="badge">Leader</span>`:c}</div></div>`)}
            ${this.cluster?.members.length?c:h`<div class="empty">Cluster topology unavailable.</div>`}
          </section>
        </section>
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
      ${this.selected?this.renderDetail(this.selected):c}
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
          <label>Type<select name="type" @change=${s=>this.channelKind=s.target.value}><option value="webhook">Webhook</option><option value="telegram">Telegram</option></select></label>
          <label>Name<input name="name" placeholder="On-call" required /></label>
          ${this.channelKind==="webhook"?h`<label>Webhook URL<input name="url" type="url" placeholder="https://hooks.example.com/upgrid" required /></label>`:h`<label>Bot token<input name="bot_token" type="password" autocomplete="off" required /></label><label>Chat ID<input name="chat_id" required /></label>`}
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("channel-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>Create channel</button></div>
        </form>
      </dialog>
      <dialog id="join-dialog" aria-labelledby="join-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="join-title">Join a node</h2><p>This command contains Cluster credentials. Keep it private.</p></div>
        <div class="join-command">${this.joinCommand}</div>
        <div class="dialog-actions" style="padding: 0 22px 22px"><button class="button secondary" @click=${()=>this.closeDialog("join-dialog")}>Close</button><button class="button" @click=${this.copyJoinCommand}>${this.copied?"Copied":"Copy command"}</button></div>
      </dialog>
    `}renderTarget(i){const e=i.latest_evaluation;return h`
      <div class="target-wrap">
        <input class="select-target" type="checkbox" aria-label=${`Select ${i.name}`} .checked=${this.selectedIds.has(i.id)} @change=${t=>this.toggleSelected(i.id,t.target.checked)} />
        <button class="target" aria-label=${i.name} @click=${()=>this.openTarget(i)}>
          <i class="state ${i.availability}" aria-label=${i.availability}></i>
          <div>
            <h3>${i.name}</h3>
            <div class="meta">${i.paused?"Paused · ":""}${i.method} · ${i.url} · every ${i.interval_seconds}s</div>
          </div>
          <div class="latency">
            <strong>${e?`${e.latency_ms} ms`:"—"}</strong>
            <span>${e?e.status_code??"network error":"waiting"}</span>
          </div>
        </button>
      </div>
    `}renderDetail(i){const e=i.accepted_statuses.map(t=>t.start===t.end?t.start:`${t.start}-${t.end}`).join(",");return h`
      <dialog id="detail-dialog" aria-labelledby="target-detail-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="target-detail-title">Target details</h2><p>${i.id}</p></div>
        <form @submit=${this.updateTarget}>
          <label>Name<input name="name" .value=${i.name} required /></label>
          <label>URL<input name="url" type="url" .value=${i.url} required /></label>
          <div class="row">
            <label>Method<input name="method" .value=${i.method} required /></label>
            <label>Expected statuses<input name="statuses" .value=${e} required /></label>
          </div>
          <div class="row">
            <label>Interval (seconds)<input name="interval" type="number" min="1" .value=${String(i.interval_seconds)} required /></label>
            <label>Timeout (seconds)<input name="timeout" type="number" min="1" .value=${String(i.timeout_seconds)} required /></label>
          </div>
          <div class="row">
            <label>Failures before Down<input name="failures" type="number" min="1" .value=${String(i.failure_threshold)} required /></label>
            <label>Maximum redirects<input name="max_redirects" type="number" min="0" .value=${String(i.max_redirects)} required /></label>
          </div>
          <label>Body must contain<input name="body_contains" .value=${i.body_contains??""} /></label>
          <div class="row">
            <label class="check"><input name="follow_redirects" type="checkbox" .checked=${i.follow_redirects} />Follow redirects</label>
            <label class="check"><input name="skip_tls_verification" type="checkbox" .checked=${i.skip_tls_verification} />Skip TLS verification</label>
          </div>
          <div class="dialog-actions">
            <button class="button danger" type="button" @click=${this.deleteTarget}>Delete target</button>
            <button class="button secondary" type="button" @click=${()=>this.setPaused(!i.paused)}>${i.paused?"Resume evaluations":"Pause evaluations"}</button>
            <button class="button secondary" type="button" @click=${this.closeDetailDialog}>Close</button>
            <button class="button" type="submit" ?disabled=${this.saving}>Save changes</button>
          </div>
        </form>
        <section class="history">
          <h3>Evaluation history</h3>
          ${i.history.length?h`<table><thead><tr><th>Time</th><th>Result</th><th>Status</th><th>Latency</th></tr></thead><tbody>${i.history.map(t=>h`<tr><td>${new Date(t.recorded_at_ms).toLocaleString()}</td><td>${t.succeeded?"Up":"Failed"}</td><td>${t.status_code??"—"}</td><td>${t.latency_ms} ms</td></tr>`)}</tbody></table>`:h`<p class="meta">No evaluations recorded yet.</p>`}
        </section>
      </dialog>
    `}};p.styles=ce`
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
      display: block;
      min-height: 100vh;
      background:
        radial-gradient(circle at 12% -5%, #18392d 0, transparent 30%),
        linear-gradient(145deg, #090d0c 0%, #0c1210 55%, #09100d 100%);
      color: var(--text);
      font: 14px/1.5 Inter, ui-sans-serif, system-ui, sans-serif;
    }
    * { box-sizing: border-box; }
    button, input, select { font: inherit; }
    .shell { max-width: 1200px; margin: auto; padding: 28px 24px 72px; }
    header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 34px; }
    .brand, .actions, .live, nav { display: flex; align-items: center; }
    .brand { gap: 13px; }
    .brand img { width: 42px; height: 42px; filter: drop-shadow(0 0 18px #40d89035); }
    .brand strong { display: block; font-size: 19px; letter-spacing: .02em; }
    .brand span, .live, .eyebrow, .meta { color: var(--muted); font-size: 12px; }
    nav { gap: 4px; padding: 4px; border: 1px solid var(--line); border-radius: 11px; background: #0d1210aa; }
    nav a { color: var(--muted); padding: 7px 11px; text-decoration: none; border-radius: 7px; }
    nav a.active { color: var(--text); background: #202b27; }
    .actions { gap: 12px; }
    .live { gap: 7px; }
    .dot { width: 7px; height: 7px; border-radius: 50%; background: var(--amber); }
    .dot.on { background: var(--green); box-shadow: 0 0 10px var(--green); }
    .heading { display: flex; align-items: flex-end; justify-content: space-between; margin-bottom: 18px; }
    .heading h1 { margin: 2px 0 0; font-size: clamp(27px, 4vw, 38px); line-height: 1.1; letter-spacing: -.035em; }
    .eyebrow { text-transform: uppercase; letter-spacing: .16em; }
    .button { border: 1px solid #3e765a; border-radius: 9px; background: #1c4a35; color: #e8fff2; padding: 9px 13px; cursor: pointer; }
    .button:hover { border-color: #62b988; }
    .button:disabled { cursor: wait; opacity: .65; }
    .summary { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }
    .metric, .panel { border: 1px solid var(--line); background: #111715dc; box-shadow: 0 16px 48px #0002; }
    .metric { border-radius: 14px; padding: 17px 18px; }
    .metric span { display: block; color: var(--muted); font-size: 11px; letter-spacing: .11em; text-transform: uppercase; }
    .metric strong { display: block; margin-top: 5px; font-size: 29px; font-weight: 560; }
    .panel { border-radius: 16px; overflow: hidden; }
    .resources { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; margin-top: 18px; }
    .resource { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 13px 20px; border-bottom: 1px solid #202925; }
    .resource:last-child { border-bottom: 0; }
    .resource strong { display: block; font-size: 13px; }
    .resource code { color: var(--muted); font-size: 11px; }
    .badge { border: 1px solid #3c554a; border-radius: 999px; color: #a7c3b7; padding: 2px 7px; font-size: 10px; text-transform: uppercase; }
    .panel-head { display: flex; align-items: center; justify-content: space-between; padding: 17px 20px; border-bottom: 1px solid var(--line); }
    .panel-head h2 { margin: 0; font-size: 14px; }
    .target-wrap { display: grid; grid-template-columns: auto minmax(0, 1fr); align-items: center; border-bottom: 1px solid #202925; padding-left: 20px; }
    .target-wrap:last-child { border-bottom: 0; }
    .select-target { width: 15px; height: 15px; accent-color: var(--green); }
    .target { width: 100%; display: grid; grid-template-columns: auto minmax(0, 1fr) auto; gap: 14px; align-items: center; padding: 17px 20px 17px 14px; border: 0; background: transparent; color: var(--text); text-align: left; cursor: pointer; }
    .target-wrap:hover, .target-wrap:hover .target { background: #17201c; }
    .state { width: 10px; height: 10px; border-radius: 50%; background: var(--amber); box-shadow: 0 0 12px currentColor; }
    .state.up { color: var(--green); background: var(--green); }
    .state.down { color: var(--red); background: var(--red); }
    .target h3 { margin: 0 0 3px; font-size: 14px; }
    .meta { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .latency { text-align: right; }
    .latency strong { display: block; font-weight: 500; }
    .latency span { color: var(--muted); font-size: 11px; }
    .empty { padding: 54px 20px; color: var(--muted); text-align: center; }
    .notice { margin: 0 0 16px; border: 1px solid #7b3937; border-radius: 10px; background: #391b1a; color: #ffb3af; padding: 10px 12px; }
    .toolbar { display: grid; grid-template-columns: minmax(180px, 1fr) auto auto; gap: 8px; padding: 12px 20px; border-bottom: 1px solid var(--line); }
    .toolbar input, .toolbar select { padding: 7px 9px; }
    .bulk { display: flex; align-items: center; gap: 8px; padding: 10px 20px; border-bottom: 1px solid var(--line); background: #16221d; }
    dialog { width: min(580px, calc(100% - 28px)); border: 1px solid var(--line); border-radius: 17px; background: var(--panel); color: var(--text); padding: 0; box-shadow: 0 28px 90px #000b; }
    dialog::backdrop { background: #040706cc; backdrop-filter: blur(5px); }
    .dialog-head { padding: 20px 22px 15px; border-bottom: 1px solid var(--line); }
    .dialog-head h2 { margin: 0; font-size: 18px; }
    .dialog-head p { margin: 4px 0 0; color: var(--muted); }
    form { display: grid; gap: 13px; padding: 20px 22px 22px; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    label { display: grid; gap: 5px; color: var(--muted); font-size: 11px; letter-spacing: .03em; }
    input, select { width: 100%; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: #0c110f; color: var(--text); padding: 9px 10px; }
    input:focus, select:focus { border-color: #4b936c; }
    .dialog-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 5px; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .danger { margin-right: auto; background: transparent; color: #ff9b97; border-color: #633b39; }
    .check { display: flex; align-items: center; gap: 8px; }
    .check input { width: auto; }
    .history { margin: 0 22px 22px; border-top: 1px solid var(--line); padding-top: 18px; }
    .history h3 { margin: 0 0 10px; font-size: 14px; }
    .join-command { margin: 20px 22px; border: 1px solid var(--line); border-radius: 10px; background: #0b110e; color: var(--green); padding: 13px; overflow-wrap: anywhere; font: 12px/1.6 ui-monospace, SFMono-Regular, monospace; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th, td { padding: 7px 5px; border-bottom: 1px solid #202925; text-align: left; }
    th { color: var(--muted); font-weight: 500; }
    @media (max-width: 720px) {
      .shell { padding: 20px 14px 60px; }
      nav { display: none; }
      .summary { grid-template-columns: 1fr 1fr; }
      .resources { grid-template-columns: 1fr; }
      .toolbar { grid-template-columns: 1fr 1fr; }
      .toolbar input { grid-column: 1 / -1; }
      .heading { align-items: flex-start; gap: 16px; }
      .target { grid-template-columns: auto minmax(0, 1fr); }
      .latency { grid-column: 2; text-align: left; }
    }
  `;g([m()],p.prototype,"targets",2);g([m()],p.prototype,"channels",2);g([m()],p.prototype,"alerts",2);g([m()],p.prototype,"secrets",2);g([m()],p.prototype,"cluster",2);g([m()],p.prototype,"error",2);g([m()],p.prototype,"live",2);g([m()],p.prototype,"saving",2);g([m()],p.prototype,"selected",2);g([m()],p.prototype,"channelKind",2);g([m()],p.prototype,"joinCommand",2);g([m()],p.prototype,"search",2);g([m()],p.prototype,"statusFilter",2);g([m()],p.prototype,"sort",2);g([m()],p.prototype,"selectedIds",2);g([m()],p.prototype,"activeSection",2);g([m()],p.prototype,"copied",2);p=g([Oe("upgrid-app")],p);
